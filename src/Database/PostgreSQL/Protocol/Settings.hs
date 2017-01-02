{-# language OverloadedStrings #-}

module Database.PostgreSQL.Protocol.Settings where

import Data.Word (Word16)
import Data.ByteString (ByteString)

data ConnectionSettings = ConnectionSettings
    { connHost     :: ByteString
    , connPort     :: Word16
    , connDatabase :: ByteString
    , connUser     :: ByteString
    , connPassword :: ByteString
    } deriving (Show)

defaultConnectionSettings :: ConnectionSettings
defaultConnectionSettings = ConnectionSettings
    { connHost     = ""
    , connPort     = 5432
    , connDatabase = "postgres"
    , connUser     = "postgres"
    , connPassword = ""
    }

