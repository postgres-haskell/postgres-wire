module Database.PostgreSQL.Driver.Query where

import Control.Concurrent.Chan.Unagi
import Data.Foldable
import qualified Data.Vector as V
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.StatementStorage

-- Public
data Query = Query
    { qStatement    :: B.ByteString
    , qValues       :: V.Vector (Oid, B.ByteString)
    , qParamsFormat :: Format
    , qResultFormat :: Format
    , qCachePolicy  :: CachePolicy
    } deriving (Show)

-- | Public
sendBatch :: Connection -> [Query] -> IO ()
sendBatch conn = traverse_ sendSingle
  where
    s = connRawConnection conn
    sname = StatementName ""
    pname = PortalName ""
    sendSingle q = do
        sendMessage s $
            Parse sname (StatementSQL $ qStatement q) (fst <$> qValues q)
        sendMessage s $
            Bind pname sname (qParamsFormat q) (snd <$> qValues q)
                (qResultFormat q)
        sendMessage s $ Execute pname noLimitToReceive

-- | Public
sendBatchAndSync :: Connection -> [Query] -> IO ()
sendBatchAndSync conn qs = sendBatch conn qs >> sendSync conn

-- | Public
sendBatchAndFlush :: Connection -> [Query] -> IO ()
sendBatchAndFlush conn qs = sendBatch conn qs >> sendFlush conn

sendSync :: Connection -> IO ()
sendSync conn = sendMessage (connRawConnection conn) Sync

sendFlush :: Connection -> IO ()
sendFlush conn = sendMessage (connRawConnection conn) Flush

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
readReadyForQuery = fmap (liftError . findFirstError)
                    . collectBeforeReadyForQuery
  where
    liftError = maybe (Right ()) (Left . PostgresError)

findFirstError :: [ServerMessage] -> Maybe ErrorDesc
findFirstError []                       = Nothing
findFirstError (ErrorResponse desc : _) = Just desc
findFirstError (_ : xs)                 = findFirstError xs

-- Collects all messages received before ReadyForQuery
collectBeforeReadyForQuery :: Connection -> IO [ServerMessage]
collectBeforeReadyForQuery conn = do
    msg <- readChan $ connOutAllChan conn
    case msg of
        ReadForQuery{} -> pure []
        m              -> (m:) <$> collectBeforeReadyForQuery conn

-- | Public
describeStatement
    :: Connection
    -> B.ByteString
    -> IO (Either Error (V.Vector Oid, V.Vector FieldDescription))
describeStatement conn stmt = do
    sendMessage s $ Parse sname (StatementSQL stmt) V.empty
    sendMessage s $ DescribeStatement sname
    sendMessage s Sync
    parseMessages <$> collectBeforeReadyForQuery conn
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

