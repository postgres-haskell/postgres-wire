{-# language BangPatterns #-}
{-# language LambdaCase #-}
module Main where

import Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.ByteString.Builder (toLazyByteString)
import Data.Vector as V(fromList, empty)
import Data.IORef
import Data.Int
import Data.Foldable
import Data.Maybe
import Control.Concurrent
import Control.Applicative
import Control.Monad
import Data.Monoid
import System.Clock
import Options.Applicative

import qualified Database.PostgreSQL.LibPQ as LibPQ

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.DataRows
import Database.PostgreSQL.Driver
--
-- CREATE TABLE _bytes_100_of_1k(b bytea);
-- CREATE TABLE _bytes_400_of_200(b bytea);
-- CREATE TABLE _bytes_10_of_20k(b bytea);
-- CREATE TABLE _bytes_1_of_200(b bytea);
-- CREATE TABLE _bytes_300_of_100(b bytea);

-- INSERT INTO _bytes_100_of_1k(b)
--   (SELECT repeat('a', 1000)::bytea FROM generate_series(1, 100));
--
-- INSERT INTO _bytes_400_of_200(b)
--   (SELECT repeat('a', 200)::bytea FROM generate_series(1, 400));
--
-- INSERT INTO _bytes_10_of_20k(b)
--   (SELECT repeat('a', 20000)::bytea FROM generate_series(1, 10));
--
-- INSERT INTO _bytes_1_of_200(b) VALUES(repeat('a', 200)::bytea);
--
-- INSERT INTO _bytes_300_of_100(b)
--  (SELECT repeat('a', 100)::bytea FROM generate_series(1, 300));

data Action 
    = BenchPW RowsType
    | BenchLibPQ RowsType
    | BenchLoop
    deriving (Show, Eq)

data RowsType
    = Bytes100_1k
    | Bytes400_200
    | Bytes10_20k
    | Bytes1_200
    | Bytes300_100
    deriving (Show, Eq)

cli :: Parser Action
cli = hsubparser $
       cmd "pw" "benchmark postgres-wire" (BenchPW <$> rowTypeParser)
    <> cmd "libpq" "benchmark libpq" (BenchLibPQ <$> rowTypeParser) 
    <> cmd "loop" "benchmark datarows decoding loop" (pure BenchLoop)
  where
    cmd c h p = command c (info (helper <*> p) $ header h)
    rowTypeParser = hsubparser $
           cmd "b100_1k"  "100 rows of 1k bytes"  (pure Bytes100_1k)
        <> cmd "b400_200" "400 rows of 200 bytes" (pure Bytes400_200)
        <> cmd "b10_20k"  "10 rows of 20k bytes"  (pure Bytes10_20k)
        <> cmd "b1_200"   "1 row of 200 bytes"    (pure Bytes1_200)
        <> cmd "b300_100" "300 rows of 100 bytes" (pure Bytes300_100)  

main :: IO ()
main = execParser (info (helper <*> cli) $ header "Postgres-wire benchmark") 
        >>= execAction

execAction :: Action -> IO ()
execAction (BenchPW rows)    = benchPw $ queryStatement rows
execAction (BenchLibPQ rows) = benchLibpq $ queryStatement rows
execAction BenchLoop         = benchLoop

queryStatement :: RowsType -> B.ByteString
queryStatement = \case
    Bytes100_1k  -> "SELECT * from _bytes_100_of_1k"
    Bytes400_200 -> "SELECT * from _bytes_400_of_200"
    Bytes10_20k  -> "SELECT * from _bytes_10_of_20k"
    Bytes1_200   -> "SELECT * fromm _bytes_1_of_200"
    Bytes300_100 -> "SELECT * from _bytes_300_of_100"

benchPw :: B.ByteString -> IO ()
benchPw statement = benchRequests createConnection $ \c -> do
    sendBatchAndSync c [q]
    d <- readNextData c
    waitReadyForQuery c
  where
    q = Query statement V.empty Binary Binary AlwaysCache
    createConnection = connect defaultSettings >>= 
        either (error . ("Connection error " <>) . show) pure

    defaultSettings = defaultConnectionSettings
        { settingsHost     = "localhost"
        , settingsDatabase = "travis_test"
        , settingsUser     = "postgres"
        , settingsPassword = ""
        }

benchLibpq :: B.ByteString -> IO ()
benchLibpq statement = benchRequests libpqConnection $ \c -> do
    r <- fromJust <$> LibPQ.execPrepared c "" [] LibPQ.Binary
    rows <- LibPQ.ntuples r
    parseRows r (rows - 1)
  where
    libpqConnection = do
        conn <- LibPQ.connectdb "host=localhost user=postgres dbname=travis_test"
        LibPQ.prepare conn "" "SELECT * from _bytes_300_of_100" Nothing
        pure conn
    parseRows r (-1) = pure ()
    parseRows r n = LibPQ.getvalue r n 0 >> parseRows r (n - 1)

benchRequests :: IO c -> (c -> IO a) -> IO ()
benchRequests connectAction queryAction = do
    results <- replicateM 8 newThread
    threadDelay $ durationSeconds * 1000 * 1000
    for_ results $ \(_, _, tid) -> killThread tid
    s <- sum <$> traverse (\(ref, _, _) -> readIORef ref) results
    latency_total <- sum <$> traverse (\(_, ref, _) -> readIORef ref) results

    print $ "Requests per second: " ++ show (s `div` durationSeconds)
    print $ "Average latency, ms: " ++ displayLatency latency_total s
  where
    durationSeconds = 10
    newThread  = do
        ref_count   <- newIORef 0 :: IO (IORef Int)
        ref_latency <- newIORef 0 :: IO (IORef Int64)
        c <- connectAction
        tid <- forkIO $ forever $ do
            t1 <- getTime Monotonic
            r <- queryAction c
            r `seq` pure ()
            t2 <- getTime Monotonic
            modifyIORef' ref_latency (+ (getDifference t2 t1))
            modifyIORef' ref_count (+1)
        pure (ref_count, ref_latency, tid)

    getDifference (TimeSpec end_s end_ns) (TimeSpec start_s start_ns) = 
        (end_s - start_s) * 1000000000 + end_ns - start_ns

    displayLatency latency reqs = 
        let a = latency `div` fromIntegral reqs
            (ms, ns) = a `divMod` 1000000
        in show ms <> "." <> show ns

benchLoop :: IO ()
benchLoop = do
    counter <- newIORef 0  :: IO (IORef Word)
    content <- newIORef "" :: IO (IORef BL.ByteString)
    -- File contains a PostgreSQL binary response on the query:
    --   "SELECT typname, typnamespace, typowner, typlen, typbyval,
    --          typcategory, typispreferred, typisdefined, typdelim,
    --          typrelid, typelem, typarray from pg_type"
    !bs <- B.readFile "bench/pg_type_rows.out"
    writeIORef content . BL.cycle $ BL.fromStrict bs

    let handler dm = case dm of
            DataMessage _ -> modifyIORef' counter (+1)
            _             -> pure ()
        newChunk preBs = do
            b <- readIORef content
            let (nb, rest) = BL.splitAt 4096 b
            writeIORef content rest
            let res = preBs <> ( BL.toStrict nb)
            res `seq` pure res
    tid <- forkIO . forever $ loopExtractDataRows newChunk handler
    threadDelay $ durationSeconds * 1000 * 1000
    killThread tid
    s <- readIORef counter
    print $ "Data messages parsed per second: " 
            ++ show (s `div` fromIntegral durationSeconds)
  where
    durationSeconds = 10
