module Fault where

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
import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Protocol.Types

import Connection

longQuery :: Query
longQuery = Query "SELECT pg_sleep(5)" V.empty Text Text NeverCache

testFaults :: TestTree
testFaults = testGroup "Faults"
    [ testCase "Single batch by waitReadyForQuery" testBatchReadyForQuery
    , testCase "Single batch by readNextData " testBatchNextData
    ]

testBatchReadyForQuery :: IO ()
testBatchReadyForQuery = withConnection $ \c -> do
    sendBatchAndSync c [longQuery]
    interruptConnection c
    r <- waitReadyForQuery c
    assertUnexpected r

testBatchNextData :: IO ()
testBatchNextData  = withConnection $ \c -> do
    sendBatchAndSync c [longQuery]
    interruptConnection c
    r <- readNextData c
    assertUnexpected r

-- testSimpleQuery :: IO ()
-- testSimpleQuery = withConnection $ \c -> do
--     r <- sendSimpleQuery c "SELECT pg_sleep(5)"
--     interruptConnection c
--     assertUnexpected r

interruptConnection :: Connection -> IO ()
interruptConnection = close

assertUnexpected :: Show a => Either Error a -> Assertion
assertUnexpected (Left (UnexpectedError _)) = pure ()
assertUnexpected (Right v) = assertFailure $
    "Expected Unexpected error, but got " ++ show v

