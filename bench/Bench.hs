{-# language BangPatterns #-}
module Main where

import Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString (ByteString)
import Data.Vector as V(fromList, empty)
import Data.IORef
import Control.Concurrent
import Control.Applicative
import Control.Monad
import Data.Monoid
import Control.DeepSeq

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver
import Criterion.Main

-- CREATE TABLE _bytes_100_of_1k(b bytea);
-- CREATE TABLE _bytes_400_of_200(b bytea);
-- CREATE TABLE _bytes_10_of_20k(b bytea);
-- CREATE TABLE _bytes_1_of_200(b bytea);

-- INSERT INTO _bytes_100_of_1k(b)
--   (SELECT repeat('a', 1000)::bytea FROM generate_series(1, 100));
-- INSERT INTO _bytes_400_of_200(b)
--   (SELECT repeat('a', 200)::bytea FROM generate_series(1, 400));
-- INSERT INTO _bytes_10_of_20k(b)
--   (SELECT repeat('a', 20000)::bytea FROM generate_series(1, 10));
-- INSERT INTO _bytes_1_of_200(b) VALUES(repeat('a', 200)::bytea);

main = defaultMain
    [ bgroup "Requests"
        [ env createConnection (\c -> bench "100 of 1k" . nfIO $ requestAction c)
        ]
    ]

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
    traverse (killThread . snd) rs
    s <- sum <$> traverse (readIORef . fst) rs
    print $ "Requests: " ++ show s
  where
    newThread  = do
        ref <- newIORef 0 :: IO (IORef Word)
        c <- connectAction
        tid <- forkIO $ forever $ do
            queryAction c
            modifyIORef' ref (+1)
        pure (ref, tid)

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
    largeStmt = "SELECT * from _bytes_400_of_200"

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

