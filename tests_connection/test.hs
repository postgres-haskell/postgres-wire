{-
   Tests for connection establishment and athorization. It is assumed
   than these tests being executed with different settings in pg_hba.conf
-}
import Test.Tasty
import Test.Tasty.HUnit

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.Settings

main :: IO ()
main = defaultMain $ testGroup "Postgres-wire"
    [ testConnection
    ]

testConnection :: TestTree
testConnection = testGroup "Connection" $
    map (\(name, settings) -> testCase name $ connectAndClose settings)
        [ ("Connection to default socket", defaultSettings
            { settingsHost = "" })
        , ("Connection to Unix socket", defaultSettings
            { settingsHost = "/var/run/postgresql" })
        , ("Connection to TCP ipv4 socket", defaultSettings
            { settingsHost = "localhost" })
        ]
  where
    connectAndClose settings = connect settings >>= close
    defaultSettings = ConnectionSettings
        { settingsHost     = ""
        , settingsPort     = 5432
        , settingsDatabase = "test_connection"
        , settingsUser     = "test_postgres"
        , settingsPassword = "password"
        , settingsTls      = NoTls
        }

