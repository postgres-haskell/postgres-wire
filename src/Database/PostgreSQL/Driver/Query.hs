module Database.PostgreSQL.Driver.Query where

import Data.Foldable
import Data.Monoid
import Data.Bifunctor
import qualified Data.Vector as V
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Store.Encode
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.StatementStorage

-- Public
data Query = Query
    { qStatement    :: B.ByteString
    , qValues       :: V.Vector (Oid, Maybe B.ByteString)
    , qParamsFormat :: Format
    , qResultFormat :: Format
    , qCachePolicy  :: CachePolicy
    } deriving (Show)

-- | Public
sendBatchAndFlush :: Connection -> [Query] -> IO ()
sendBatchAndFlush = sendBatchEndBy Flush

-- | Public
sendBatchAndSync :: Connection -> [Query] -> IO ()
sendBatchAndSync = sendBatchEndBy Sync

-- | Public
sendSync :: Connection -> IO ()
sendSync = sendEncode conn $ encodeClientMessage Sync

-- | Public
sendSimpleQuery :: ConnectionCommon -> B.ByteString -> IO (Either Error ())
sendSimpleQuery conn q = do
    sendMessage (connRawConnection conn) $ SimpleQuery (StatementSQL q)
    checkErrors <$> collectUntilReadyForQuery conn
  where
    checkErrors = either
        (Left . ReceiverError)
        (maybe (Right ()) (Left . PostgresError) . findFirstError)

waitReadyForQuery :: Connection -> IO (Either Error ())
waitReadyForQuery conn =
    readChan (connOutChan conn) >>=
    either (pure . Left . ReceiverError) handleDataMessage
  where
    handleDataMessage msg = case msg of
        (DataError e) -> do
            -- We should wait for ReadyForQuery anyway.
            waitReadyForQuery conn
            pure . Left $ PostgresError e
        (DataMessage _) -> error "incorrect usage waitReadyForQuery"
        DataReady        -> pure $ Right ()

-- | Public
readNextData :: Connection -> IO (Either Error DataRows)
readNextData conn =
    readChan (connOutChan conn) >>=
    either (pure . Left . ReceiverError) handleDataMessage
  where
    handleDataMessage msg = case msg of
        (DataError e)      -> pure . Left $ PostgresError e
        (DataMessage rows) -> pure . Right $ rows
        DataReady           -> error "incorrect usage readNextData"

-- Helper
sendBatchEndBy :: ClientMessage -> Connection -> [Query] -> IO ()
sendBatchEndBy msg conn qs = do
    batch <- constructBatch conn qs
    sendEncode conn $ batch <> encodeClientMessage msg

constructBatch :: Connection -> [Query] -> IO Encode
constructBatch conn = fmap fold . traverse constructSingle
  where
    storage = connStatementStorage conn
    pname = PortalName ""
    constructSingle q = do
        let stmtSQL = StatementSQL $ qStatement q
        (sname, parseMessage) <- case qCachePolicy q of
            AlwaysCache -> do
                mName <- lookupStatement storage stmtSQL
                case mName of
                    Nothing -> do
                        newName <- storeStatement storage stmtSQL
                        pure (newName, encodeClientMessage $
                            Parse newName stmtSQL (fst <$> qValues q))
                    Just name -> pure (name, mempty)
            NeverCache -> do
                let newName = defaultStatementName
                pure (newName, encodeClientMessage $
                    Parse  newName stmtSQL (fst <$> qValues q))
        let bindMessage = encodeClientMessage $
                Bind pname sname (qParamsFormat q) (snd <$> qValues q)
                    (qResultFormat q)
            executeMessage = encodeClientMessage $
                Execute pname noLimitToReceive
        pure $ parseMessage <> bindMessage <> executeMessage

-- | Public
describeStatement
    :: ConnectionCommon
    -> B.ByteString
    -> IO (Either Error (V.Vector Oid, V.Vector FieldDescription))
describeStatement conn stmt = do
    sendEncode conn $
           encodeClientMessage (Parse sname (StatementSQL stmt) V.empty)
        <> encodeClientMessage (DescribeStatement sname)
        <> encodeClientMessage Sync
    (parseMessages =<<) . first ReceiverError <$> collectUntilReadyForQuery conn
  where
    sname = StatementName ""
    parseMessages msgs = case msgs of
        [ParameterDescription params, NoData]
            -> Right (params, V.empty)
        [ParameterDescription params, RowDescription fields]
            -> Right (params, fields)
        xs  -> Left . maybe
            -- todo handle error
             (error "handle decode error")
             PostgresError
            $ findFirstError xs

-- Collects all messages preceding `ReadyForQuery`
collectUntilReadyForQuery
    :: ConnectionCommon
    -> IO (Either ReceiverException [ServerMessage])
collectUntilReadyForQuery conn = do
    msg <- readChan $ connOutChan conn
    case msg of
        Left e               -> pure $ Left e
        Right ReadyForQuery{} -> pure $ Right []
        Right m              -> fmap (m:) <$> collectUntilReadyForQuery conn

findFirstError :: [ServerMessage] -> Maybe ErrorDesc
findFirstError []                       = Nothing
findFirstError (ErrorResponse desc : _) = Just desc
findFirstError (_ : xs)                 = findFirstError xs

