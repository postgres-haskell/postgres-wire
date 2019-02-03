module Database.PostgreSQL.Driver
    (
    -- * Settings
      ConnectionSettings(..)
    , TlsMode
    , defaultConnectionSettings
    -- * Connection
    , Connection
    , connect
    , close
    -- * Information about connection
    , getServerVersion
    , getServerEncoding
    , getIntegerDatetimes
    -- * Queries
    , Query(..)
    , Oid(..)
    , Format(..)
    , CachePolicy(..)
    , sendBatchAndSync
    , sendBatchAndFlush
    , readNextData
    , readAllData
    , waitReadyForQuery
    , sendSimpleQuery
    , describeStatement
    , findFirstError
    , findAllErrors
    -- * Errors
    , Error(..)
    , AuthError(..)
    , ErrorDesc(..)
    ) where

import Database.PostgreSQL.Protocol.Types

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.StatementStorage
import Database.PostgreSQL.Driver.Settings
import Database.PostgreSQL.Driver.Query
import Database.PostgreSQL.Driver.Error

