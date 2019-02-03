module Database.PostgreSQL.Driver.Query 
    ( Query(..)
    -- * Connection
    , sendBatchAndFlush
    , sendBatchAndSync
    , sendSync
    , readNextData
    , readAllData
    , waitReadyForQuery
    -- * Connection common
    , sendSimpleQuery
    , describeStatement
    , collectUntilReadyForQuery
    , findFirstError
    , findAllErrors
    ) where

import Control.Concurrent.STM.TQueue (TQueue, readTQueue )
import Control.Concurrent.STM        (atomically)
import Data.Foldable                 (fold)
import Data.Monoid                   ((<>))
import Data.ByteString               (ByteString)
import Data.Vector                   (Vector)

import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Store.Encode
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.StatementStorage

-- Public
data Query = Query
    { qStatement    :: !ByteString
    , qValues       :: ![(Oid, Maybe Encode)]
    , qParamsFormat :: !Format
    , qResultFormat :: !Format
    , qCachePolicy  :: !CachePolicy
    } deriving (Show)

-- | Public
{- INLINE sendBatchAndFlush #-}
sendBatchAndFlush :: Connection -> [Query] -> IO ()
sendBatchAndFlush = sendBatchEndBy Flush

-- | Public
{-# INLINE sendBatchAndSync #-}
sendBatchAndSync :: Connection -> [Query] -> IO ()
sendBatchAndSync = sendBatchEndBy Sync

-- | Public
{-# INLINE sendSync #-}
sendSync :: Connection -> IO ()
sendSync conn = sendEncode conn $ encodeClientMessage Sync

-- | Public
{-# INLINABLE readNextData #-}
readNextData :: Connection -> IO (Either Error ByteString)
readNextData conn =
    readChan (connChan conn) >>=
    either (pure . Left . ReceiverError) handleDataMessage
  where
    handleDataMessage msg = case msg of
        DataRow bs -> pure . Right $ bs
        ReadyForQuery _ -> throwIncorrectUsage "Expected DataRow, but got ReadyForQuery"
        _ -> readNextData conn

{-# INLINABLE readAllData #-}
readAllData :: Connection -> IO (Either Error [ByteString])
readAllData conn = do
  msgs <- collectUntilReadyForQuery conn
  return $ msgs >>= (\msgs -> return [bs | DataRow bs <- msgs])

{-# INLINABLE waitReadyForQuery #-}
waitReadyForQuery :: Connection -> IO (Either Error ())
waitReadyForQuery conn =
    readChan (connChan conn) >>=
    either (pure . Left . ReceiverError) handleDataMessage
  where
    handleDataMessage msg = case msg of
        ReadyForQuery _ -> pure $ Right ()
        DataRow _ -> throwIncorrectUsage "Expected ReadyForQuery, but got DataRow message"
        ErrorResponse e -> do
          waitReadyForQuery conn
          pure . Left $ PostgresError e

-- Helper
{-# INLINE sendBatchEndBy #-}
sendBatchEndBy :: ClientMessage -> Connection -> [Query] -> IO ()
sendBatchEndBy msg conn qs = do
    batch <- constructBatch conn qs
    sendEncode conn $ batch <> encodeClientMessage msg

-- INFO about invalid statement in cache
constructBatch :: Connection -> [Query] -> IO Encode
constructBatch conn = fmap fold . traverse constructSingle
  where
    storage = connStatementStorage conn
    pname = PortalName ""
    constructSingle q = do
        let stmtSQL = StatementSQL $ qStatement q
        (stmtName, needParse) <- case qCachePolicy q of
            AlwaysCache -> lookupStatement storage stmtSQL >>= \case
                Nothing     -> do
                    newName <- storeStatement storage stmtSQL
                    pure (newName, True)
                Just name   -> 
                    pure (name, False)
            NeverCache -> pure (defaultStatementName, True)
        let parseMessage = if needParse 
                then encodeClientMessage $ 
                    Parse stmtName stmtSQL (fst <$> qValues q)
                else mempty
            bindMessage = encodeClientMessage $
                Bind pname stmtName (qParamsFormat q) (snd <$> qValues q)
                    (qResultFormat q)
            executeMessage = encodeClientMessage $
                Execute pname noLimitToReceive
        pure $ parseMessage <> bindMessage <> executeMessage

-- | Public
sendSimpleQuery :: Connection -> ByteString -> IO (Either Error ())
sendSimpleQuery conn q = do
    sendMessage (connRawConnection conn) $ SimpleQuery (StatementSQL q)
    (checkErrors =<<) <$> collectUntilReadyForQuery conn
  where
    checkErrors = 
        maybe (Right ()) (Left . PostgresError) . findFirstError

-- | Public
describeStatement
    :: Connection
    -> ByteString
    -> IO (Either Error (Vector Oid, Vector FieldDescription))
describeStatement conn stmt = do
    sendEncode conn $
           encodeClientMessage (Parse sname (StatementSQL stmt) [])
        <> encodeClientMessage (DescribeStatement sname)
        <> encodeClientMessage Sync
    msgs <-  collectUntilReadyForQuery conn
    either (pure . Left) parseMessages msgs
  where
    sname = StatementName ""
    parseMessages msgs = case msgs of
        [ParameterDescription params, NoData]
            -> pure $ Right (params, mempty)
        [ParameterDescription params, RowDescription fields]
            -> pure $ Right (params, fields)
        xs  -> maybe
              (throwProtocolEx "Unexpected response on describe message")
              (pure . Left . PostgresError)
              $ findFirstError xs

-- Collects all messages preceding `ReadyForQuery`.
collectUntilReadyForQuery
    :: Connection
    -> IO (Either Error [ServerMessage])
collectUntilReadyForQuery conn = do
    msg <- readChan $ connChan conn
    case msg of
        Left e                -> pure $ Left $ ReceiverError e
        Right ReadyForQuery{} -> pure $ Right []
        Right m               -> fmap (m:) <$> collectUntilReadyForQuery conn

-- | Searches for the first ErrorResponse if it exists.
findFirstError :: [ServerMessage] -> Maybe ErrorDesc
findFirstError []                       = Nothing
findFirstError (ErrorResponse desc : _) = Just desc
findFirstError (_ : xs)                 = findFirstError xs

findAllErrors :: [ServerMessage] -> [ErrorDesc]
findAllErrors msgs = [e | ErrorResponse e <- msgs]

{-# INLINE readChan #-}
readChan :: TQueue a -> IO a
readChan = atomically . readTQueue
