module Database.PostgreSQL.Driver.Query 
    ( Query(..)
    -- * Connection
    , sendBatchAndFlush
    , sendBatchAndSync
    , sendSync
    , readNextData
    , waitReadyForQuery
    -- * Connection common
    , sendSimpleQuery
    , describeStatement
    , collectUntilReadyForQuery
    ) where

import Data.Foldable
import Data.Monoid
import Data.Bifunctor
import qualified Data.Vector as V
import qualified Data.ByteString as B
import Control.Concurrent.STM.TQueue (TQueue, readTQueue )
import Control.Concurrent.STM (atomically)

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
sendSync conn = sendEncode conn $ encodeClientMessage Sync

-- | Public
readNextData :: Connection -> IO (Either Error DataRows)
readNextData conn =
    readChan (connOutChan conn) >>=
    either (pure . Left . ReceiverError) handleDataMessage
  where
    handleDataMessage msg = case msg of
        (DataError e)      -> pure . Left $ PostgresError e
        (DataMessage rows) -> pure . Right $ rows
        DataReady          -> throwIncorrectUsage
            "Expected DataRow message, but got ReadyForQuery"

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
        (DataMessage _)   -> throwIncorrectUsage
            "Expected ReadyForQuery, but got DataRow message"
        DataReady        -> pure $ Right ()

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
sendSimpleQuery :: ConnectionCommon -> B.ByteString -> IO (Either Error ())
sendSimpleQuery conn q = do
    sendMessage (connRawConnection conn) $ SimpleQuery (StatementSQL q)
    (checkErrors =<<) <$> collectUntilReadyForQuery conn
  where
    checkErrors = 
        maybe (Right ()) (Left . PostgresError) . findFirstError

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
    msgs <-  collectUntilReadyForQuery conn
    either (pure . Left) parseMessages msgs
  where
    sname = StatementName ""
    parseMessages msgs = case msgs of
        [ParameterDescription params, NoData]
            -> pure $ Right (params, V.empty)
        [ParameterDescription params, RowDescription fields]
            -> pure $ Right (params, fields)
        xs  -> maybe
              (throwProtocolEx "Unexpected response on describe message")
              (pure . Left . PostgresError)
              $ findFirstError xs

-- Collects all messages preceding `ReadyForQuery`.
collectUntilReadyForQuery
    :: ConnectionCommon
    -> IO (Either Error [ServerMessage])
collectUntilReadyForQuery conn = do
    msg <- readChan $ connOutChan conn
    case msg of
        Left e                -> pure $ Left $ ReceiverError e
        Right ReadyForQuery{} -> pure $ Right []
        Right m               -> fmap (m:) <$> collectUntilReadyForQuery conn

-- | Searches for the first ErrorResponse if it exists.
findFirstError :: [ServerMessage] -> Maybe ErrorDesc
findFirstError []                       = Nothing
findFirstError (ErrorResponse desc : _) = Just desc
findFirstError (_ : xs)                 = findFirstError xs

readChan :: TQueue a -> IO a
readChan = atomically . readTQueue
