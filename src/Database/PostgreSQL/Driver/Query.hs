module Database.PostgreSQL.Driver.Query where

import Control.Concurrent.Chan.Unagi
import Data.Foldable
import Data.Monoid
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
sendBatchAndSync :: Connection -> [Query] -> IO ()
sendBatchAndSync = sendBatchEndBy Sync

-- | Public
sendBatchAndFlush :: Connection -> [Query] -> IO ()
sendBatchAndFlush = sendBatchEndBy Flush

-- Helper
sendBatchEndBy :: ClientMessage -> Connection -> [Query] -> IO ()
sendBatchEndBy msg conn qs = do
    batch <- constructBatch conn qs
    sendEncode (connRawConnection conn) $ batch <> encodeClientMessage msg

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
readNextData :: Connection -> IO (Either Error DataMessage)
readNextData conn = readChan $ connOutDataChan conn

-- | Public
sendSimpleQuery :: Connection -> B.ByteString -> IO (Either Error ())
sendSimpleQuery conn q = withConnectionMode conn SimpleQueryMode $ \c -> do
    sendMessage (connRawConnection c) $ SimpleQuery (StatementSQL q)
    readReadyForQuery c


-- | Public
-- SHOULD BE called after every sended `Sync` message
-- skips all messages except `ReadyForQuery`
readReadyForQuery :: Connection -> IO (Either Error ())
readReadyForQuery = fmap (>>= (liftError . findFirstError))
                    . collectBeforeReadyForQuery
  where
    liftError = maybe (Right ()) (Left . PostgresError)

findFirstError :: [ServerMessage] -> Maybe ErrorDesc
findFirstError []                       = Nothing
findFirstError (ErrorResponse desc : _) = Just desc
findFirstError (_ : xs)                 = findFirstError xs

-- Collects all messages received before ReadyForQuery
collectBeforeReadyForQuery :: Connection -> IO (Either Error [ServerMessage])
collectBeforeReadyForQuery conn = do
    msg <- readChan $ connOutAllChan conn
    case msg of
        Left e               -> pure $ Left e
        Right ReadForQuery{} -> pure $ Right []
        Right m              -> fmap (m:) <$> collectBeforeReadyForQuery conn

-- | Public
describeStatement
    :: Connection
    -> B.ByteString
    -> IO (Either Error (V.Vector Oid, V.Vector FieldDescription))
describeStatement conn stmt = do
    sendEncode s $
           encodeClientMessage (Parse sname (StatementSQL stmt) V.empty)
        <> encodeClientMessage (DescribeStatement sname)
        <> encodeClientMessage Sync
    (parseMessages =<<) <$> collectBeforeReadyForQuery conn
  where
    s = connRawConnection conn
    sname = StatementName ""
    parseMessages msgs = case msgs of
        [ParameterDescription params, NoData]
            -> Right (params, V.empty)
        [ParameterDescription params, RowDescription fields]
            -> Right (params, fields)
        xs  -> maybe (error "Impossible happened") (Left . PostgresError )
               $ findFirstError xs

