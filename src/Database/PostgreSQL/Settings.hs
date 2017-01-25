{-# language OverloadedStrings #-}

module Database.PostgreSQL.Settings where

import Data.Word (Word16)
import Data.ByteString (ByteString)

-- | Connection settings to PostgreSQL
data ConnectionSettings = ConnectionSettings
    { -- On empty string a Unix socket will be used.
      settingsHost     :: ByteString
    , settingsPort     :: Word16
    , settingsDatabase :: ByteString
    , settingsUser     :: ByteString
    , settingsPassword :: ByteString
    } deriving (Show)

defaultConnectionSettings :: ConnectionSettings
defaultConnectionSettings = ConnectionSettings
    { settingsHost     = ""
    , settingsPort     = 5432
    , settingsDatabase = "testdb"
    , settingsUser     = "v"
    , settingsPassword = ""
    }

