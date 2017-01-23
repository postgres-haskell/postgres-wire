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
import System.Socket hiding (connect, close)
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
    -- Chan for only data messages
    , connDataOutChan        :: OutChan (Either Error DataMessage)
    -- Chan for all messages that filter
    , connAllOutChan         :: OutChan ServerMessage
    , connStatementStorage   :: StatementStorage
    , connParameters         :: ConnectionParameters
    }

newtype ServerMessageFilter = ServerMessageFilter (ServerMessage -> Bool)

type NotificationHandler = Notification -> IO ()

-- All possible errors
data Error
    = PostgresError ErrorDesc
    | ImpossibleError

data DataMessage = DataMessage B.ByteString

address :: SocketAddress Unix
address = fromJust $ socketAddressUnixPath "/var/run/postgresql/.s.PGSQL.5432"

connect :: ConnectionSettings -> IO Connection
connect settings = do
    s <- socket
    Socket.connect s address
    sendStartMessage s $ consStartupMessage settings
    r <- receive s 4096 mempty
    readAuthMessage r

    (inChan, outChan) <- newChan
    tid <- forkIO $ receiverThread s inChan
    storage <- newStatementStorage
    pure Connection
        { connSocket = s
        , connReceiverThread = tid
        , connOutChan = outChan
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

receiverThread :: UnixSocket -> InChan ServerMessage -> IO ()
receiverThread sock chan = forever $ do
    r <- receive sock 4096 mempty
    print r
    go r
  where
    decoder = runGetIncremental decodeServerMessage
    go str = case pushChunk decoder str of
        BG.Done rest _ v -> do
            putStrLn $ "Received: " ++ show v
            unless (B.null rest) $ go rest
        BG.Partial _ -> error "Partial"
        BG.Fail _ _ e -> error e
    dispatch :: ServerMessage -> IO ()
    -- dont receiving at this phase
    dispatch (BackendKeyData _ _) = pure ()
    dispatch (BindComplete) = pure ()
    dispatch CloseComplete = pure ()
    -- maybe return command result too
    dispatch (CommandComplete _) = pure ()
    dispatch r@(DataRow _) = writeChan chan r
    -- TODO throw error here
    dispatch EmptyQueryResponse = pure ()
    -- TODO throw error here
    dispatch (ErrorResponse desc) = pure ()
    -- TODO
    dispatch NoData = pure ()
    dispatch (NoticeResponse _) = pure ()
    -- TODO handle notifications
    dispatch (NotificationResponse n) = pure ()
    -- Ignore here ?
    dispatch (ParameterDescription _) = pure ()
    dispatch (ParameterStatus _ _) = pure ()
    dispatch (ParseComplete) = pure ()
    dispatch (PortalSuspended) = pure ()
    dispatch (ReadForQuery _) = pure ()
    dispatch (RowDescription _) = pure ()

data Query = Query
    { qStatement :: B.ByteString
    , qOids :: V.Vector Oid
    , qValues :: V.Vector B.ByteString
    , qParamsFormat :: Format
    , qResultFormat :: Format
    } deriving (Show)

query1 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["1", "3"] Text Text
query2 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["2", "3"] Text Text
query3 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["3", "3"] Text Text
query4 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["4", "3"] Text Text
-- query5 = Query "SELECT * FROM a whereee v > $1 + $2 LIMIT 100" [Oid 23, Oid 23] ["5", "3"]

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
    sendBatch c [query1, query2, query3, query4, query5]
    threadDelay $ 1 * 1000 * 1000
    close c


-- sendBatchAndSync :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatchAndSync = undefined

-- sendBatchAndFlush :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatchAndFlush = undefined

-- internal helper
-- sendBatch :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatch = undefined

-- readNextData :: Connection -> IO Data?
-- readNextData = undefined
--
-- readNextServerMessage ?
--
--
-- Simple Queries support or maybe dont support it
-- because single text query may be send through extended protocol
-- may be support for all standalone queries

-- data Request = forall a . Request (QQuery a)

