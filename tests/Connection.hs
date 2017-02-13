module Connection where

import Control.Exception (bracket)
import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.Settings

-- | Creates a connection.
withConnection :: (Connection -> IO a) -> IO a
withConnection = bracket (getConnection <$> connect defaultSettings) close

-- | Creates a common connection.
withConnectionCommon :: (ConnectionCommon -> IO a) -> IO a
withConnectionCommon = bracket
    (getConnection <$> connectCommon defaultSettings) close

-- | Creates connection than collects all server messages in chan.
withConnectionCommonAll :: (ConnectionCommon -> IO a) -> IO a
withConnectionCommonAll = bracket
    (getConnection <$> connectCommon' defaultSettings filterAllowedAll) close

defaultSettings = defaultConnectionSettings
    { settingsHost     = "localhost"
    , settingsDatabase = "travis_test"
    , settingsUser     = "postgres"
    , settingsPassword = ""
    }

getConnection :: Either Error (AbsConnection c)-> AbsConnection c
getConnection (Left e) = error $ "Connection error " ++ show e
getConnection (Right c) = c

