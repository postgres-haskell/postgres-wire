{-# language OverloadedLists #-}
{-# language OverloadedStrings #-}
{-# language GADTs #-}
{-# language ApplicativeDo #-}
{-# language ExistentialQuantification #-}
{-# language TypeSynonymInstances #-}
{-# language FlexibleInstances #-}
module Database.PostgreSQL.Protocol.Connection where


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

import Database.PostgreSQL.Protocol.Settings
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.StatementStorage


type UnixSocket = Socket Unix Stream Unix
-- data Connection = Connection (Socket Inet6 Stream TCP)
data Connection = Connection
    { connSocket             :: UnixSocket
    , connReceiverThread     :: ThreadId
    , connOutChan            :: OutChan ServerMessage
    , connStatementStorage   :: StatementStorage
    , connParameters         :: ConnectionParameters
    }

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
    -- putStrLn "sending message:"
    -- print smsg
    send sock smsg mempty

sendMessage :: UnixSocket -> ClientMessage -> IO ()
sendMessage sock msg = void $ do
    let smsg = toStrict . toLazyByteString $ encodeClientMessage msg
    -- putStrLn "sending message:"
    -- print smsg
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
    go r
  where
    decoder = runGetIncremental decodeServerMessage
    go str = case pushChunk decoder str of
        BG.Done rest _ v -> do
            print v
            writeChan chan v
            unless (B.null rest) $ go rest
        BG.Partial _ -> error "Partial"
        BG.Fail _ _ e -> error e

data Query = Query
    { qStatement :: B.ByteString
    , qOids :: V.Vector Oid
    , qValues :: V.Vector B.ByteString
    } deriving (Show)

query1 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["1", "3"]
query2 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["2", "3"]
query3 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["3", "3"]
query4 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["4", "3"]
query5 = Query "SELECT * FROM a where v > $1 + $2 LIMIT 100" [Oid 23, Oid 23] ["5", "3"]
-- query1 = QQuery "test1" "select sum(v) from a" [] []
-- query2 = QQuery "test2" "select sum(v) from a" [] []
-- query3 = QQuery "test3" "select sum(v) from a" [] []
-- query4 = QQuery "test4" "select sum(v) from a" [] []
-- query5 = QQuery "test5" "select sum(v) from a" [] []

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
        sendMessage s $ Bind pname sname Text (qValues q) Text
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
-- Simple Queries support or maybe dont support it
-- because single text query may be send through extended protocol
-- may be support for all standalone queries

-- data Request = forall a . Request (QQuery a)

