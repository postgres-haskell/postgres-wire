{-# language BangPatterns #-}
module Main where

import Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString (ByteString)
import Data.Vector as V(fromList, empty)
import Data.IORef
import Data.Int
import Data.Foldable
import Data.Maybe
import Control.Concurrent
import Control.Applicative
import Control.Monad
import Data.Monoid
import Control.DeepSeq
import System.IO.Unsafe
import System.Clock

import qualified Database.PostgreSQL.LibPQ as LibPQ

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Codecs.Decoders
import Database.PostgreSQL.Protocol.ExtractDataRows
import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver
import Criterion.Main

-- CREATE TABLE _bytes_100_of_1k(b bytea);
-- CREATE TABLE _bytes_400_of_200(b bytea);
-- CREATE TABLE _bytes_10_of_20k(b bytea);
-- CREATE TABLE _bytes_1_of_200(b bytea);
-- CREATE TABLE _bytes_300_of_100(b bytea);

-- INSERT INTO _bytes_100_of_1k(b)
--   (SELECT repeat('a', 1000)::bytea FROM generate_series(1, 100));
-- INSERT INTO _bytes_400_of_200(b)
--   (SELECT repeat('a', 200)::bytea FROM generate_series(1, 400));
-- INSERT INTO _bytes_10_of_20k(b)
--   (SELECT repeat('a', 20000)::bytea FROM generate_series(1, 10));
-- INSERT INTO _bytes_1_of_200(b) VALUES(repeat('a', 200)::bytea);
-- INSERT INTO _bytes_300_of_100(b)
--  (SELECT repeat('a', 100)::bytea FROM generate_series(1, 300));

-- main = benchMultiPw
-- main = defaultMain
--     [ bgroup "Requests"
--         [ 
--             -- env createConnection (\c -> bench "100 of 1k" . nfIO $ requestAction c)
--             bench "parser" $ nf parse bs
--         ]
--     ]
main = benchMultiPw

{-# NOINLINE bs #-}
bs :: B.ByteString
bs = unsafePerformIO $ B.readFile "1.txt"

parse bs | B.null bs = ()
         | otherwise = let (rest, v) = runDecode getCustomRow bs
                       in v `seq` parse rest

benchLoop :: IO ()
benchLoop = do
    ref <- newIORef 0 :: IO (IORef Word)
    rbs <- newIORef "" :: IO (IORef BL.ByteString)
    !bs <- B.readFile "1.txt"
    let str = BL.cycle $ BL.fromStrict bs
    writeIORef rbs str

    let handler dm = case dm of
            DataMessage _ -> modifyIORef' ref (+1)
            _ -> pure ()
        newChunk preBs = do
            b <- readIORef rbs
            let (nb, rest) = BL.splitAt 4096 b
            writeIORef rbs rest
            -- let res = preBs <> (B.copy $ BL.toStrict nb)
            let res = preBs <> ( BL.toStrict nb)
            res `seq` pure res
    tid <- forkIO $ forever $ loopExtractDataRows newChunk handler
    threadDelay 1000000
    killThread tid
    s <- readIORef ref
    print $ "Requests: " ++ show s

benchRequests :: IO c -> (c -> IO a) -> IO ()
benchRequests connectAction queryAction = do
    rs <- replicateM 8 newThread
    threadDelay 10000000
    traverse (\(_,_, tid) -> killThread tid) rs
    s <- sum <$> traverse (\(ref, _, _) -> readIORef ref) rs
    latency_total <- sum <$> traverse (\(_, ref, _) -> readIORef ref) rs
    print $ "Requests: " ++ show s
    print $ "Average latency: " ++ show (latency_total `div` fromIntegral s)
  where
    newThread  = do
        ref_count <- newIORef 0 :: IO (IORef Word)
        ref_latency <- newIORef 0 :: IO (IORef Int64)
        c <- connectAction
        tid <- forkIO $ forever $ do
            t1 <- getTime Monotonic
            queryAction c
            t2 <- getTime Monotonic
            modifyIORef' ref_latency (+ (getDifference t2 t1))
            modifyIORef' ref_count (+1)
        pure (ref_count, ref_latency, tid)

getDifference (TimeSpec end_s end_ns) (TimeSpec start_s start_ns) = 
    (end_s - start_s) * 1000000000 + end_ns - start_ns

requestAction c = replicateM_ 100 $ do
            sendBatchAndSync c [q]
            readNextData c
            waitReadyForQuery c
  where
    q = Query largeStmt V.empty Binary Binary AlwaysCache
    largeStmt = "SELECT * from _bytes_1_of_200"

benchMultiPw :: IO ()
benchMultiPw = benchRequests createConnection $ \c -> do
            sendBatchAndSync c [q]
            readNextData c
            waitReadyForQuery c
  where
    q = Query largeStmt V.empty Binary Binary AlwaysCache
    largeStmt = "SELECT * from _bytes_300_of_100"

benchLibpq :: IO ()
benchLibpq = benchRequests libpqConnection $ \c -> do
    r <- fromJust <$> LibPQ.execPrepared c "" [] LibPQ.Binary
    rows <- LibPQ.ntuples r
    go r (rows - 1)
  where
    libpqConnection = do
        conn <- LibPQ.connectdb "host=localhost user=postgres dbname=travis_test"
        LibPQ.prepare conn "" "SELECT * from _bytes_300_of_100" Nothing
        pure conn
    go r (-1) = pure ()
    go r n = LibPQ.getvalue r n 0 >> go r (n - 1)


-- Connection
-- | Creates connection with default filter.
createConnection :: IO Connection
createConnection = getConnection <$> connect defaultSettings

getConnection :: Either Error Connection -> Connection
getConnection (Left e) = error $ "Connection error " ++ show e
getConnection (Right c) = c

defaultSettings = defaultConnectionSettings
    { settingsHost     = "localhost"
    , settingsDatabase = "travis_test"
    , settingsUser     = "postgres"
    , settingsPassword = ""
    }

-- Orphans

instance NFData (AbsConnection a) where
    rnf _ = ()

instance NFData Error where
    rnf _ = ()

