{-# language OverloadedStrings #-}

module Database.PostgreSQL.Settings where

import Data.Word (Word16)
import Data.ByteString (ByteString)

-- | Connection settings to PostgreSQL
data ConnectionSettings = ConnectionSettings
    { -- Host maybe IP-address or hostname.
      -- If starts with slash, it recognized as directory where unix socket
      -- file is located. Format is dir/.s.PGSQL.nnnn, where nnnn is port
      -- number.
      -- On empty string default unix socket path will be used
      -- Only ipv4 is supported now.
      settingsHost     :: ByteString
    , settingsPort     :: Word16
    , settingsDatabase :: ByteString
    , settingsUser     :: ByteString
    , settingsPassword :: ByteString
    } deriving (Show)

defaultConnectionSettings :: ConnectionSettings
defaultConnectionSettings = ConnectionSettings
    { settingsHost     = "localhost"
    , settingsPort     = 5432
    , settingsDatabase = "testdb"
    , settingsUser     = "v"
    , settingsPassword = ""
    }

