{-
   Tests for connection establishment and athorization. It is assumed
   than these tests being executed with different settings in pg_hba.conf
-}
import Control.Exception (finally)
import Data.Monoid ((<>))
import Data.Foldable
import qualified Data.ByteString as B
import System.Process (callCommand)
import Test.Tasty
import Test.Tasty.HUnit

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.Settings

main :: IO ()
main = defaultMain $ testGroup "Postgres-wire"
    [ testConnection "Connection trust" confTrust
    , testConnection "Connection with password" confPassword
    , testConnection "Connection with md5" confPassword
    ]

testConnection :: TestName -> B.ByteString -> TestTree
testConnection name confContent = testCase name $ withPghba confContent $
    traverse_ connectAndClose
        [ defaultSettings { settingsHost = "" }
        , defaultSettings { settingsHost = "/var/run/postgresql" }
        , defaultSettings { settingsHost = "localhost" }
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

pghbaFilename :: FilePath
pghbaFilename = "/etc/postgresql/9.5/main/pg_hba.conf"

withPghba :: B.ByteString -> IO a -> IO a
withPghba confContent action = do
    oldContent <- B.readFile pghbaFilename
    B.writeFile pghbaFilename confContent
    (restart >> action) `finally`
        (B.writeFile pghbaFilename oldContent >> restart)
  where
    restart = callCommand "service postgresql restart"

makeConf :: B.ByteString -> B.ByteString
makeConf method =
     "local all   postgres                     peer\n"
  <> "local all   all                          " <> method <> "\n"
  <> "host  all   all             127.0.0.1/32 " <> method <> "\n"
  <> "host  all   all             ::1/128      " <> method <> "\n"

confTrust :: B.ByteString
confTrust = makeConf "trust"

confPassword :: B.ByteString
confPassword = makeConf "password"

confMd5 :: B.ByteString
confMd5 = makeConf "md5"

