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

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Driver
import Criterion.Main

main = benchLoop

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

benchMultiPw :: IO ()
benchMultiPw = benchRequests createConnection $ \c -> do
            sendBatchAndSync c [q, q, q, q, q, q, q, q, q, q]
            readNextData c
            readNextData c
            readNextData c
            readNextData c
            readNextData c
            readNextData c
            readNextData c
            readNextData c
            readNextData c
            readNextData c
            waitReadyForQuery c
  where
    q = Query largeStmt V.empty Binary Binary AlwaysCache
    largeStmt = "select typname, typnamespace, typowner, typlen, typbyval,"
                <> "typcategory, typispreferred, typisdefined, typdelim,"
                <> "typrelid, typelem, typarray from pg_type "

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

