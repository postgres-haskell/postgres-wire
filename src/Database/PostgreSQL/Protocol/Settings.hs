{-# language OverloadedStrings #-}

module Database.PostgreSQL.Protocol.Settings where

import Data.Word (Word16)
import Data.ByteString (ByteString)

data ConnectionSettings = ConnectionSettings
    { settingsHost     :: ByteString
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

