module Driver where

import Data.Monoid ((<>))
import Data.Foldable
import Control.Monad
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS

import Test.Tasty
import Test.Tasty.HUnit

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Protocol.Types

import Connection

makeQuery1 :: B.ByteString -> Query
makeQuery1 n = Query "SELECT $1" [Oid 23] [n] Text Text

makeQuery2 :: B.ByteString -> B.ByteString -> Query
makeQuery2 n1 n2 = Query "SELECT $1 + $2" [Oid 23, Oid 23] [n1, n2] Text Text

testDriver = testGroup "Driver"
    [ testCase "Single batch" testBatch
    , testCase "Two batches" testTwoBatches
    ]

fromRight (Right v) = v
fromRight _         = error "fromRight"

testBatch :: IO ()
testBatch = withConnection $ \c -> do
    let a = "5"
        b = "3"
    sendBatchAndSync c [makeQuery1 a, makeQuery1 b]
    readReadyForQuery c

    r1 <- readNextData c
    r2 <- readNextData c
    DataMessage [[a]] @=? fromRight r1
    DataMessage [[b]] @=? fromRight r2

testTwoBatches :: IO ()
testTwoBatches = withConnection $ \c -> do
    let a = 7
        b = 2
    sendBatchAndFlush c [ makeQuery1 (BS.pack (show a))
                        , makeQuery1 (BS.pack (show b))]
    r1 <- fromMessage . fromRight <$> readNextData c
    r2 <- fromMessage . fromRight <$> readNextData c

    sendBatchAndSync c [makeQuery2 r1 r2]
    r <- readNextData c
    readReadyForQuery c

    DataMessage [[BS.pack (show $ a + b)]] @=? fromRight r
  where
    fromMessage (DataMessage [[v]]) = v
    fromMessage _                   = error "from message"

