{-# language OverloadedLists #-}
{-# language OverloadedStrings #-}
{-# language GADTs #-}
{-# language ApplicativeDo #-}
{-# language ExistentialQuantification #-}
{-# language TypeSynonymInstances #-}
{-# language FlexibleInstances #-}
module Database.PostgreSQL.Connection where


import qualified Data.ByteString as B
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Control.Monad
import Data.Traversable
import Data.Foldable
import Control.Applicative
import Data.Monoid
import Control.Concurrent (forkIO, killThread, ThreadId, threadDelay)
import Data.Binary.Get ( runGetIncremental, pushChunk)
import qualified Data.Binary.Get as BG (Decoder(..))
import Data.Maybe (fromJust)
import qualified Data.Vector as V
import System.Socket hiding (connect, close, Error(..))
import qualified System.Socket as Socket (connect, close)
import System.Socket.Family.Inet6
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP
import System.Socket.Family.Unix
import Data.Time.Clock.POSIX
import Control.Concurrent.Chan.Unagi

import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Settings
import Database.PostgreSQL.StatementStorage
import Database.PostgreSQL.Types


type UnixSocket = Socket Unix Stream Unix
-- data Connection = Connection (Socket Inet6 Stream TCP)
data Connection = Connection
    { connSocket             :: UnixSocket
    , connReceiverThread     :: ThreadId
    -- channel only for Data messages
    , connOutDataChan        :: OutChan (Either Error DataMessage)
    -- channel for all the others messages
    , connOutAllChan         :: OutChan ServerMessage
    , connStatementStorage   :: StatementStorage
    , connParameters         :: ConnectionParameters
    }

type ServerMessageFilter = ServerMessage -> Bool

type NotificationHandler = Notification -> IO ()

-- All possible errors
data Error
    = PostgresError ErrorDesc
    | ImpossibleError
    deriving (Show)

data DataMessage = DataMessage [V.Vector B.ByteString]
    deriving (Show)


address :: SocketAddress Unix
address = fromJust $ socketAddressUnixPath "/var/run/postgresql/.s.PGSQL.5432"

connect :: ConnectionSettings -> IO Connection
connect settings = do
    s <- socket
    Socket.connect s address
    sendStartMessage s $ consStartupMessage settings
    r <- receive s 4096 mempty
    readAuthMessage r

    (inDataChan, outDataChan) <- newChan
    (inAllChan, outAllChan) <- newChan
    tid <- forkIO $ receiverThread s inDataChan inAllChan
    storage <- newStatementStorage
    pure Connection
        { connSocket = s
        , connReceiverThread = tid
        , connOutDataChan = outDataChan
        , connOutAllChan = outAllChan
        , connStatementStorage = storage
        , connParameters = ConnectionParameters
            { paramServerVersion = ServerVersion 1 1 1
            , paramServerEncoding = ""
            , paramIntegerDatetimes = True
            }
        }

close :: Connection -> IO ()
close conn = do
    killThread $ connReceiverThread conn
    Socket.close $ connSocket conn

consStartupMessage :: ConnectionSettings -> StartMessage
consStartupMessage stg = StartupMessage
    (Username $ settingsUser stg) (DatabaseName $ settingsDatabase stg)

sendStartMessage :: UnixSocket -> StartMessage -> IO ()
sendStartMessage sock msg = void $ do
    let smsg = toStrict . toLazyByteString $ encodeStartMessage msg
    send sock smsg mempty

sendMessage :: UnixSocket -> ClientMessage -> IO ()
sendMessage sock msg = void $ do
    let smsg = toStrict . toLazyByteString $ encodeClientMessage msg
    send sock smsg mempty

readAuthMessage :: B.ByteString -> IO ()
readAuthMessage s =
    case pushChunk (runGetIncremental decodeAuthResponse) s of
        BG.Done _ _ r -> case r of
            AuthenticationOk -> putStrLn "Auth ok"
            _                -> error "Invalid auth"
        f -> error $ show s

receiverThread
    :: UnixSocket
    -> InChan (Either Error DataMessage)
    -> InChan ServerMessage
    -> IO ()
receiverThread sock dataChan allChan = receiveLoop []
  where
    receiveLoop :: [V.Vector B.ByteString] -> IO()
    receiveLoop acc = do
        r <- receive sock 4096 mempty
        -- print r
        go r acc >>= receiveLoop

    decoder = runGetIncremental decodeServerMessage
    go :: B.ByteString -> [V.Vector B.ByteString] -> IO [V.Vector B.ByteString]
    go str acc = case pushChunk decoder str of
        BG.Done rest _ v -> do
            -- putStrLn $ "Received: " ++ show v
            -- TODO select filter
            when (defaultFilter v) $ writeChan allChan v
            newAcc <- dispatch v acc
            if B.null rest
                then pure newAcc
                else go rest newAcc
        BG.Partial _ -> error "Partial"
        BG.Fail _ _ e -> error e

    dispatch
        :: ServerMessage
        -> [V.Vector B.ByteString]
        -> IO [V.Vector B.ByteString]
    -- Command is completed, return the result
    dispatch (CommandComplete _) acc = do
        writeChan dataChan . Right . DataMessage $ reverse acc
        pure []
    -- note that data rows go in reversed order
    dispatch (DataRow row) acc = pure (row:acc)
    -- PostgreSQL sends this if query string was empty and datarows should be
    -- empty, but anyway we return data collected in `acc`.
    dispatch EmptyQueryResponse acc = do
        writeChan dataChan . Right . DataMessage $ reverse acc
        pure []
    -- On ErrorResponse we should discard all the collected datarows
    dispatch (ErrorResponse desc) acc = do
        writeChan dataChan $ Left $ PostgresError desc
        pure []
    -- TODO handle notifications
    dispatch (NotificationResponse n) acc = pure acc
    -- We does not handled this case because we always send `execute`
    -- with no limit.
    dispatch PortalSuspended acc = pure acc
    -- do nothing on other messages
    dispatch _ acc = pure acc

defaultFilter :: ServerMessageFilter
defaultFilter msg = case msg of
    -- PostgreSQL sends it only in startup phase
    BackendKeyData{}       -> False
    -- just ignore
    BindComplete           -> False
    -- just ignore
    CloseComplete          -> False
    -- messages affecting data handled in dispatcher
    CommandComplete{}      -> False
    -- messages affecting data handled in dispatcher
    DataRow{}              -> False
    -- messages affecting data handled in dispatcher
    EmptyQueryResponse     -> False
    -- We need collect all errors to know whether the whole command is successful
    ErrorResponse{}        -> True
    -- We need to know if the server send NoData on `describe` message
    NoData                 -> True
    -- All notices are not showing
    NoticeResponse{}       -> False
    -- notifications will be handled by callbacks or in a separate channel
    NotificationResponse{} -> False
    -- As result for `describe` message
    ParameterDescription{} -> True
    -- we dont store any run-time parameter that is not a constant
    ParameterStatus{}      -> False
    -- just ignore
    ParseComplete          -> False
    -- messages affecting data handled in dispatcher
    PortalSuspended        -> False
    -- to know when command processing is finished
    ReadForQuery{}         -> True
    -- as result for `describe` message
    RowDescription{}       -> True

data Query = Query
    { qStatement    :: B.ByteString
    , qOids         :: V.Vector Oid
    , qValues       :: V.Vector B.ByteString
    , qParamsFormat :: Format
    , qResultFormat :: Format
    } deriving (Show)

query1 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["1", "3"] Text Text
query2 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["a", "3"] Text Text
query3 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["3", "3"] Text Text
query4 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["4", "3"] Text Text

sendBatch :: Connection -> [Query] -> IO ()
sendBatch conn qs = do
    traverse sendSingle $ take 5 qs
    sendMessage s Sync
  where
    s = connSocket conn
    sendSingle q = do
        let sname = StatementName ""
            pname = PortalName ""
        sendMessage s $ Parse sname (StatementSQL $ qStatement q) (qOids q)
        sendMessage s $
            Bind pname sname (qParamsFormat q) (qValues q) (qResultFormat q)
        sendMessage s $ Execute pname noLimitToReceive


test :: IO ()
test = do
    c <- connect defaultConnectionSettings
    sendBatch c queries
    readResults c $ length queries
    readReadyForQuery c >>= print
    close c
  where
    queries = [query1, query2, query3, query4 ]
    readResults c 0 = pure ()
    readResults c n = do
        r <- readNextData c
        print r
        case r of
            Left  _ -> pure ()
            Right _ -> readResults c $ n - 1

-- sendBatchAndSync :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatchAndSync = undefined

-- sendBatchAndFlush :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatchAndFlush = undefined

-- internal helper
-- sendBatch :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatch = undefined

readNextData :: Connection -> IO (Either Error DataMessage)
readNextData conn = readChan $ connOutDataChan conn

-- SHOULD BE called after every sended `Sync` message
-- skips all messages except `ReadyForQuery`
readReadyForQuery :: Connection -> IO (Either Error ())
readReadyForQuery = fmap (liftError . findFirstError)
                    . waitReadyForQueryCollect
  where
    liftError = maybe (Right ()) (Left . PostgresError)

findFirstError :: [ServerMessage] -> Maybe ErrorDesc
findFirstError []                       = Nothing
findFirstError (ErrorResponse desc : _) = Just desc
findFirstError (_ : xs)                 = findFirstError xs

-- Collects all messages received before ReadyForQuery
waitReadyForQueryCollect :: Connection -> IO [ServerMessage]
waitReadyForQueryCollect conn = do
    msg <- readChan $ connOutAllChan conn
    case msg of
        ReadForQuery{} -> pure []
        m              -> (m:) <$> waitReadyForQueryCollect conn

describeStatement
    :: Connection
    -> StatementSQL
    -> IO (Either Error (V.Vector Oid, V.Vector FieldDescription))
describeStatement conn stmt = do
    sendMessage s $ Parse sname stmt []
    sendMessage s $ DescribeStatement sname
    sendMessage s Sync
    parseMessages <$> waitReadyForQueryCollect conn
  where
    s = connSocket conn
    sname = StatementName ""
    parseMessages msgs = case msgs of
        [ParameterDescription params, NoData]
            -> Right (params, [])
        [ParameterDescription params, RowDescription fields]
            -> Right (params, fields)
        xs  -> maybe (error "Impossible happened") (Left . PostgresError )
               $ findFirstError xs

testDescribe1 :: IO ()
testDescribe1 = do
    c <- connect defaultConnectionSettings
    r <- describeStatement c $ StatementSQL "start transaction"
    print r
    close c

testDescribe2 :: IO ()
testDescribe2 = do
    c <- connect defaultConnectionSettings
    r <- describeStatement c $ StatementSQL "select count(*) from a where v > $1"
    print r
    close c

