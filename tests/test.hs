import Test.Tasty
import Test.Tasty.HUnit

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.Settings
import Database.PostgreSQL.Protocol.Types

main :: IO ()
main = defaultMain $ testGroup "Postgres-wire"
    [ testCase "test1" test
    , testCase "test2" testDescribe1
    , testCase "test3" testDescribe2
    ]

defaultSettings = defaultConnectionSettings
    { settingsHost     = "localhost"
    , settingsDatabase = "travis_test"
    , settingsUser     = "postgres"
    , settingsPassword = ""
    }

query1 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["1", "3"] Text Text
query2 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["a", "3"] Text Text
query3 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["3", "3"] Text Text
query4 = Query "SELECT $1 + $2" [Oid 23, Oid 23] ["4", "3"] Text Text


test :: IO ()
test = do
    c <- connect defaultConnectionSettings
    sendBatch c queries
    sendSync c
    readResults c $ length queries
    readReadyForQuery c >>= print
    close c
  where
    queries = [query1, query2, query3, query4 ]
    readResults c 0 = pure ()
    readResults c n = do
        r <- readNextData c
        print r
        case r of
            Left  _ -> pure ()
            Right _ -> readResults c $ n - 1



testDescribe1 :: IO ()
testDescribe1 = do
    c <- connect defaultConnectionSettings
    r <- describeStatement c $ StatementSQL "start transaction"
    print r
    close c

testDescribe2 :: IO ()
testDescribe2 = do
    c <- connect defaultConnectionSettings
    r <- describeStatement c $ StatementSQL "select count(*) from a where v > $1"
    print r
    close c

