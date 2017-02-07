module Driver where

import Data.Monoid ((<>))
import Data.Foldable
import Control.Monad
import Data.Maybe
import Data.Either
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS
import qualified Data.Vector as V

import Test.Tasty
import Test.Tasty.HUnit

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.StatementStorage
import Database.PostgreSQL.Driver.Query
import Database.PostgreSQL.Protocol.Types

import Connection

testDriver :: TestTree
testDriver = testGroup "Driver"
    [ testCase "Single batch" testBatch
    , testCase "Two batches" testTwoBatches
    , testCase "Multiple batches" testMultipleBatches
    , testCase "Empty query" testEmptyQuery
    , testCase "Query without result" testQueryWithoutResult
    , testCase "Invalid queries" testInvalidBatch
    , testCase "Describe statement" testDescribeStatement
    , testCase "Describe statement with no data" testDescribeStatementNoData
    , testCase "Describe empty statement" testDescribeStatementEmpty
    , testCase "SimpleQuery" testSimpleQuery
    , testCase "SimpleAndExtendedQuery" testSimpleAndExtendedQuery
    , testCase "PreparedStatementCache" testPreparedStatementCache
    ]

makeQuery1 :: B.ByteString -> Query
makeQuery1 n = Query "SELECT $1" (V.fromList [(Oid 23, Just n)])
                    Text Text AlwaysCache

makeQuery2 :: B.ByteString -> B.ByteString -> Query
makeQuery2 n1 n2 = Query "SELECT $1 + $2"
    (V.fromList [(Oid 23, Just n1), (Oid 23, Just n2)]) Text Text AlwaysCache

fromRight :: Either e a -> a
fromRight (Right v) = v
fromRight _         = error "fromRight"

fromMessage :: Either e DataMessage -> B.ByteString
fromMessage (Right (DataMessage [v])) = fromJust $ V.head v
fromMessage _                     = error "from message"

-- | Single batch.
testBatch :: IO ()
testBatch = withConnection $ \c -> do
    let a = "5"
        b = "3"
    sendBatchAndSync c [makeQuery1 a, makeQuery1 b]
    readReadyForQuery c

    r1 <- readNextData c
    r2 <- readNextData c
    a @=? fromMessage r1
    b @=? fromMessage r2

-- | Two batches in single transaction.
testTwoBatches :: IO ()
testTwoBatches = withConnection $ \c -> do
    let a = 7
        b = 2
    sendBatchAndFlush c [ makeQuery1 (BS.pack (show a))
                        , makeQuery1 (BS.pack (show b))]
    r1 <- fromMessage <$> readNextData c
    r2 <- fromMessage <$> readNextData c

    sendBatchAndSync c [makeQuery2 r1 r2]
    r <- readNextData c
    readReadyForQuery c

    BS.pack (show $ a + b) @=? fromMessage r

-- | Multiple batches with individual transactions in single connection.
testMultipleBatches :: IO ()
testMultipleBatches = withConnection $ replicateM_ 10 . assertSingleBatch
  where
    assertSingleBatch c = do
        let a = "5"
            b = "6"
        sendBatchAndSync c [ makeQuery1 a, makeQuery1 b]
        r1 <- readNextData c
        a @=? fromMessage r1
        r2 <- readNextData c
        b @=? fromMessage r2
        readReadyForQuery c

-- | Query is empty string.
testEmptyQuery :: IO ()
testEmptyQuery = assertQueryNoData $
    Query "" V.empty Text Text NeverCache

-- | Query than returns no datarows.
testQueryWithoutResult :: IO ()
testQueryWithoutResult = assertQueryNoData $
    Query "SET client_encoding TO UTF8" V.empty Text Text NeverCache

-- | Asserts that query returns no data rows.
assertQueryNoData :: Query -> IO ()
assertQueryNoData q = withConnection $ \c -> do
    sendBatchAndSync c [q]
    r <- fromRight <$> readNextData c
    readReadyForQuery c
    DataMessage [] @=? r

-- | Asserts that all the received data rows are in form (Right _)
checkRightResult :: Connection -> Int -> Assertion
checkRightResult conn 0 = pure ()
checkRightResult conn n = readNextData conn >>=
    either (const $ assertFailure "Result is invalid")
           (const $ checkRightResult conn (n - 1))

-- | Asserts that (Left _) as result exists in the received data rows.
checkInvalidResult :: Connection -> Int -> Assertion
checkInvalidResult conn 0 = assertFailure "Result is right"
checkInvalidResult conn n = readNextData conn >>=
    either (const $ pure ())
           (const $ checkInvalidResult conn (n -1))

-- | Diffirent invalid queries in batches.
testInvalidBatch :: IO ()
testInvalidBatch = do
    let rightQuery = makeQuery1 "5"
        q1 = Query "SEL $1" (V.fromList [(Oid 23, Just "5")]) Text Text NeverCache
        q2 = Query "SELECT $1" (V.fromList [(Oid 23, Just "a")])  Text Text NeverCache
        q4 = Query "SELECT $1" (V.fromList [])  Text Text NeverCache

    assertInvalidBatch "Parse error" [q1]
    assertInvalidBatch "Invalid param" [ q2]
    assertInvalidBatch "Missed oid of param" [ q4]
    assertInvalidBatch "Parse error" [rightQuery, q1]
    assertInvalidBatch "Invalid param" [rightQuery, q2]
    assertInvalidBatch "Missed oid of param" [rightQuery, q4]
  where
    assertInvalidBatch desc qs = withConnection $ \c -> do
        sendBatchAndSync c qs
        readReadyForQuery c
        checkInvalidResult c $ length qs

-- | Describes usual statement.
testDescribeStatement :: IO ()
testDescribeStatement = withConnection $ \c -> do
    r <- describeStatement c $
               "select typname, typnamespace, typowner, typlen, typbyval,"
            <> "typcategory, typispreferred, typisdefined, typdelim, typrelid,"
            <> "typelem, typarray from pg_type where typtypmod = $1 "
            <> "and typisdefined = $2"
    assertBool "Should be Right" $ isRight r

-- | Describes statement that returns no data.
testDescribeStatementNoData :: IO ()
testDescribeStatementNoData = withConnection $ \c -> do
    r <- fromRight <$> describeStatement c "SET client_encoding TO UTF8"
    assertBool "Should be empty" $ V.null (fst r)
    assertBool "Should be empty" $ V.null (snd r)

-- | Describes statement that is empty string.
testDescribeStatementEmpty :: IO ()
testDescribeStatementEmpty = withConnection $ \c -> do
    r <- fromRight <$> describeStatement c ""
    assertBool "Should be empty" $ V.null (fst r)
    assertBool "Should be empty" $ V.null (snd r)

-- | Query using simple query protocol.
testSimpleQuery :: IO ()
testSimpleQuery = withConnection $ \c -> do
    r <- sendSimpleQuery c $
               "DROP TABLE IF EXISTS a;"
            <> "CREATE TABLE a(v int);"
            <> "INSERT INTO a VALUES (1), (2), (3);"
            <> "SELECT * FROM a;"
            <> "DROP TABLE a;"
    assertBool "Should be Right" $ isRight r

-- | Simple and extended queries in a sinle connection.
testSimpleAndExtendedQuery :: IO ()
testSimpleAndExtendedQuery = withConnection $ \c -> do
    let a = "7"
        b = "2"
        d = "5"
    sendBatchAndSync c [ makeQuery1 a , makeQuery1 b]
    readReadyForQuery c
    checkRightResult c 2

    rs <- sendSimpleQuery c "SELECT * FROM generate_series(1, 10)"
    assertBool "Should be Right" $ isRight rs

    sendBatchAndSync c [makeQuery1 d]
    fr <- readReadyForQuery c
    assertBool "Should be Right" $ isRight fr
    r <- fromMessage <$> readNextData c
    r @=? d

-- | Test that cache of statements works.
testPreparedStatementCache :: IO ()
testPreparedStatementCache  = withConnection $ \c -> do
    let a = 7
        b = 2
    sendBatchAndSync c [ makeQuery1 (BS.pack (show a))
                        , makeQuery1 (BS.pack (show b))
                        , makeQuery2 (BS.pack (show a)) (BS.pack (show b))]
    readReadyForQuery c
    r1 <- fromMessage <$> readNextData c
    r2 <- fromMessage <$> readNextData c
    r3 <- fromMessage <$> readNextData c

    BS.pack (show a) @=? r1
    BS.pack (show b) @=? r2
    BS.pack (show $ a + b) @=? r3

    size <- getCacheSize $ connStatementStorage c
    -- 2 different statements were send
    2 @=? size

