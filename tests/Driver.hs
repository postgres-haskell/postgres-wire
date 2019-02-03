module Driver where

import Data.Monoid ((<>))
import Data.Foldable
import Control.Monad
import Data.Maybe
import Data.Int
import Data.Either hiding (fromRight)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BS
import qualified Data.Vector as V

import Test.Tasty
import Test.Tasty.HUnit

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.StatementStorage
import Database.PostgreSQL.Driver.Query
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.DataRows
import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Decoders

import Database.PostgreSQL.Protocol.Codecs.Decoders
import Database.PostgreSQL.Protocol.Codecs.Encoders as PE

import Connection

testDriver :: TestTree
testDriver = testGroup "Driver"
    [ testCase "Single batch" testBatch
    , testCase "Two batches" testTwoBatches
    , testCase "Multiple batches" testMultipleBatches
    , testCase "Empty query" testEmptyQuery
    , testCase "Query without result" testQueryWithoutResult
    , testCase "Invalid queries" testInvalidBatch
    , testCase "Valid after postgres error" testValidAfterError
    , testCase "Describe statement" testDescribeStatement
    , testCase "Describe statement with no data" testDescribeStatementNoData
    , testCase "Describe empty statement" testDescribeStatementEmpty
    , testCase "SimpleQuery" testSimpleQuery
    , testCase "PreparedStatementCache" testPreparedStatementCache
    , testCase "Query with large response" testLargeQuery
    , testCase "Correct datarows" testCorrectDatarows
    ]

makeQuery1 :: B.ByteString -> Query
makeQuery1 n = Query "SELECT $1" [(Oid 23, Just $ PE.bytea n )] 
    Text Text AlwaysCache

makeQuery2 :: B.ByteString -> B.ByteString -> Query
makeQuery2 n1 n2 = Query "SELECT $1 + $2"
    [(Oid 23, Just $ PE.bytea n1), (Oid 23, Just $ PE.bytea n2)] 
    Text Text AlwaysCache

fromRight :: Either e a -> a
fromRight (Right v) = v
fromRight _         = error "fromRight"

fromMessage :: Either e B.ByteString -> B.ByteString
-- TODO
-- 2 bytes -count, 4 bytes - length
fromMessage (Right row) = B.drop 6 $ row
fromMessage _            = error "from message"

-- | Single batch.
testBatch :: IO ()
testBatch = withConnection $ \c -> do
    let a = "5"
        b = "3"
    sendBatchAndSync c [makeQuery1 a, makeQuery1 b]

    r1 <- readNextData c
    r2 <- readNextData c
    waitReadyForQuery c
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
    waitReadyForQuery c

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
        waitReadyForQuery c

-- | Query is empty string.
testEmptyQuery :: IO ()
testEmptyQuery = assertQueryNoData $
    Query "" [] Text Text NeverCache

-- | Query than returns no datarows.
testQueryWithoutResult :: IO ()
testQueryWithoutResult = assertQueryNoData $
    Query "SET client_encoding TO UTF8" [] Text Text NeverCache

-- | Asserts that query returns no data rows.
assertQueryNoData :: Query -> IO ()
assertQueryNoData q = withConnection $ \c -> do
    sendBatchAndSync c [q]
    r <- fromRight <$> readAllData c
    [] @=? r

-- | Asserts that all the received data messages are in form (Right _)
checkRightResult :: Connection -> Int -> Assertion
checkRightResult conn 0 = pure ()
checkRightResult conn n = readNextData conn >>=
    either (const $ assertFailure "Result is invalid")
           (const $ checkRightResult conn (n - 1))

-- | Asserts that (Left _) as result exists in the received data messages.
checkInvalidResult :: Connection -> Int -> Assertion
checkInvalidResult conn 0 = assertFailure "Result is right"
checkInvalidResult conn n = do
    msgs <- collectUntilReadyForQuery conn
    let r = (length . findAllErrors) <$> msgs
    either (const $ assertFailure "ReceiverError") (\x -> assertBool "Got errors" (x > 0)) r


-- | Diffirent invalid queries in batches.
testInvalidBatch :: IO ()
testInvalidBatch = do
    let rightQuery = makeQuery1 "5"
        q1 = Query "SEL $1" [(Oid 23, Just $ PE.bytea "5")] 
            Text Text NeverCache
        q2 = Query "SELECT $1" [(Oid 23, Just $ PE.bytea  "a")] 
            Text Text NeverCache
        q4 = Query "SELECT $1" [] Text Text NeverCache

    assertInvalidBatch "Parse error" [q1]
    assertInvalidBatch "Invalid param" [ q2]
    assertInvalidBatch "Missed oid of param" [ q4]
    assertInvalidBatch "Parse error" [rightQuery, q1]
    assertInvalidBatch "Invalid param" [rightQuery, q2]
    assertInvalidBatch "Missed oid of param" [rightQuery, q4]
  where
    assertInvalidBatch desc qs = withConnection $ \c -> do
        sendBatchAndSync c qs
        checkInvalidResult c $ length qs

-- | Connection remains valid even after PostgreSQL returned error on the 
-- previous query.
testValidAfterError :: IO ()
testValidAfterError = withConnection $ \c -> do
    let a = "5"
        rightQuery   = makeQuery1 a
        invalidQuery = Query "SELECT $1" []  Text Text NeverCache
    sendBatchAndSync c [invalidQuery]
    checkInvalidResult c 1

    sendBatchAndSync c [rightQuery]
    r <- readNextData c
    waitReadyForQuery c
    a @=? fromMessage r

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
    assertBool "Should be empty" $ null (fst r)
    assertBool "Should be empty" $ null (snd r)

-- | Describes statement that is empty string.
testDescribeStatementEmpty :: IO ()
testDescribeStatementEmpty = withConnection $ \c -> do
    r <- fromRight <$> describeStatement c ""
    assertBool "Should be empty" $ null (fst r)
    assertBool "Should be empty" $ null (snd r)

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

-- | Test that cache of statements works.
testPreparedStatementCache :: IO ()
testPreparedStatementCache  = withConnection $ \c -> do
    let a = 7
        b = 2
    sendBatchAndSync c [ makeQuery1 (BS.pack (show a))
                        , makeQuery1 (BS.pack (show b))
                        , makeQuery2 (BS.pack (show a)) (BS.pack (show b))]
    r1 <- fromMessage <$> readNextData c
    r2 <- fromMessage <$> readNextData c
    r3 <- fromMessage <$> readNextData c
    waitReadyForQuery c

    BS.pack (show a) @=? r1
    BS.pack (show b) @=? r2
    BS.pack (show $ a + b) @=? r3

    size <- getCacheSize $ connStatementStorage c
    -- 2 different statements were send
    2 @=? size

-- | Test that large responses are properly handled
testLargeQuery :: IO ()
testLargeQuery = withConnection $ \c -> do
    sendBatchAndSync c [Query largeStmt [] Text Text NeverCache ]
    r <- readAllData c
    assertBool "Should be Right" $ isRight r
  where
    largeStmt = "select typname, typnamespace, typowner, typlen, typbyval,"
                <> "typcategory, typispreferred, typisdefined, typdelim,"
                <> "typrelid, typelem, typarray from pg_type "

testCorrectDatarows :: IO ()
testCorrectDatarows = withConnection $ \c -> do
    let stmt = "SELECT * FROM generate_series(1, 1000)"
    sendBatchAndSync c [Query stmt [] Text Text NeverCache]
    r <- readAllData c
    case r of
        Left e -> error $ show e
        Right rows -> do
            map (BS.pack . show ) [1 .. 1000] @=? (decodeManyRows decodeDataRow rows)
            countDataRows rows @=? 1000
  where
    -- TODO Right parser later
    decodeDataRow :: Decode B.ByteString
    decodeDataRow = do
        getInt16BE
        getByteString . fromIntegral =<< getInt32BE
