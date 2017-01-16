{-# language OverloadedLists #-}
{-# language OverloadedStrings #-}
{-# language DeriveFunctor #-}
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
import Control.Concurrent
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

import Database.PostgreSQL.Protocol.Settings
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.StatementStorage


type UnixSocket = Socket Unix Stream Unix
-- data Connection = Connection (Socket Inet6 Stream TCP)
-- TODO add statement storage
data Connection = Connection UnixSocket ThreadId

address :: SocketAddress Unix
address = fromJust $ socketAddressUnixPath "/var/run/postgresql/.s.PGSQL.5432"

connect :: ConnectionSettings -> IO Connection
connect settings = do
    s <- socket
    Socket.connect s address
    sendMessage s $ encodeStartMessage $ consStartupMessage settings
    r <- receive s 4096 mempty
    readAuthMessage r

    tid <- forkIO $ receiverThread s
    pure $ Connection s tid

close :: Connection -> IO ()
close (Connection s tid) = do
    killThread tid
    Socket.close s

consStartupMessage :: ConnectionSettings -> StartMessage
consStartupMessage stg = StartupMessage (connUser stg) (connDatabase stg)

sendMessage :: UnixSocket -> Builder -> IO ()
sendMessage sock msg = void $ do
    let smsg = toStrict $ toLazyByteString msg
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

receiverThread :: UnixSocket -> IO ()
receiverThread sock = forever $ do
    r <- receive sock 4096 mempty
    print "Receive time"
    getPOSIXTime >>= print
    print r
    go r
  where
    decoder = runGetIncremental decodeServerMessage
    go str = case pushChunk decoder str of
        BG.Done rest _ v -> do
            print v
            unless (B.null rest) $ go rest
        BG.Partial _ -> error "Partial"
        BG.Fail _ _ e -> error e

data QQuery a = QQuery
    { qStmt :: B.ByteString
    , qOids :: V.Vector Oid
    , qValues :: V.Vector B.ByteString
    } deriving Show

-- query1 = QQuery "test1" "SELECT $1 + $2" [23, 23] ["1", "3"]
-- query2 = QQuery "test2" "SELECT $1 + $2" [23, 23] ["2", "3"]
-- query3 = QQuery "test3" "SELECT $1 + $2" [23, 23] ["3", "3"]
-- query4 = QQuery "test4" "SELECT $1 + $2" [23, 23] ["4", "3"]
-- query5 = QQuery "test5" "SELECT $1 + $2" [23, 23] ["5", "3"]
-- query1 = QQuery "test1" "select sum(v) from a" [] []
-- query2 = QQuery "test2" "select sum(v) from a" [] []
-- query3 = QQuery "test3" "select sum(v) from a" [] []
-- query4 = QQuery "test4" "select sum(v) from a" [] []
-- query5 = QQuery "test5" "select sum(v) from a" [] []

-- sendBatch :: Connection -> [QQuery] -> IO ()
-- sendBatch (Connection s _) qs = do
--     traverse sendSingle $ take 5 qs
--     sendMessage s $ encodeClientMessage Sync
--   where
--     sendSingle q = do
--         sendMessage s $ encodeClientMessage $
--             Parse (qName q) (qStmt q) (qOids q)
--         sendMessage s $ encodeClientMessage $
--             Bind (qName q) (qName q) Text (qValues q) Text
--         sendMessage s $ encodeClientMessage $ Execute (qName q)


-- sendQuery :: Connection -> IO ()
-- sendQuery (Connection s _) = do
--     sendMessage s $ encodeClientMessage $ Parse "test" "SELECT $1 + $2" [23, 23]
--     sendMessage s $ encodeClientMessage $
--         Bind "test" "test" Text ["2", "3"] Text
--     sendMessage s $ encodeClientMessage $ Execute "test"
--     sendMessage s $ encodeClientMessage Sync

-- test :: IO ()
-- test = do
--     c <- connect defaultConnectionSettings
--     -- sendQuery c
--     getPOSIXTime >>= \t -> print "Start " >> print t
--     sendBatch c [query1, query2, query3, query4, query5]
--     threadDelay $ 5 * 1000 * 1000
--     close c


-- sendBatchAndSync :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatchAndSync = undefined

-- sendBatchAndFlush :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatchAndFlush = undefined

-- internal helper
-- sendBatch :: IsQuery a => [a] -> Connection -> IO ()
-- sendBatch = undefined

-- Session Monad
--

data Request = forall a . Request (QQuery a)

query :: Decode a => QQuery a -> Session a
query q = Send One [Request q] $ Receive Done

data Count = One | Many
    deriving (Eq, Show)

data Session a
    = Done a
    | forall r . Decode r => Receive (r -> Session a)
    | Send Count [Request] (Session a)

instance Functor Session where
    f `fmap` (Done a) = Done $ f a
    f `fmap` (Receive g) = Receive $ fmap f . g
    f `fmap` (Send n br c) = Send n br (f <$> c)

instance Applicative Session where
    pure = Done

    f <*> x = case (f, x) of
        (Done g, Done y) -> Done (g y)
        (Done g, Receive next) -> Receive $ fmap g . next
        (Done g, Send n br c) -> Send n br (g <$> c)

        (Send n br c, Done y) -> Send n br (c <*> pure y)
        (Send n br c, Receive next)
            -> Send n br $ c <*> Receive next
        (Send n1 br1 c1, Send n2 br2 c2)
            -> if n1 == One
               then Send n2 (br1 <> br2) (c1 <*> c2)
               else Send n1 br1 (c1 <*> Send n2 br2 c2)

        (Receive next1, Receive next2) ->
            Receive  $ (\g -> Receive $ (g <*> ) . next2) . next1
        (Receive next, Done y) -> Receive $ (<*> Done y) . next
        (Receive next, Send n br c)
            -> Receive $ (<*> Send n br c) . next

instance Monad Session where
    return = pure

    m >>= f = case m of
        Done a -> f a
        Receive g -> Receive $ (>>=f) . g
        Send _n br c -> Send Many br (c >>= f)

    (>>) = (*>)

runSession :: Show a => Session a -> IO a
runSession (Done x) = do
    putStrLn $ "Return " ++ show x
    pure x
runSession (Receive f) = do
    putStrLn "Receiving"
    -- TODO receive here
    -- x <- receive
    -- runProgram (f $ decode x)
    undefined
runSession (Send _ rs c) = do
    putStrLn "Sending requests "
    -- TODO send requests here in batch
    runSession c


-- Type classes
class Decode a where
    decode :: String -> a

instance Decode Integer where
    decode = read

instance Decode String where
    decode = id

