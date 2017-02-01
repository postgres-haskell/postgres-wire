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
    , sendBatch
    , sendBatchAndSync
    , sendBatchAndFlush
    , sendSync
    , sendFlush
    , readNextData
    , readReadyForQuery
    , sendSimpleQuery
    , describeStatement
    -- * Errors
    , Error(..)
    , AuthError(..)
    , ErrorDesc(..)
    ) where

import Database.PostgreSQL.Protocol.Types

import Database.PostgreSQL.Driver.Connection
import Database.PostgreSQL.Driver.Settings
import Database.PostgreSQL.Driver.Query
import Database.PostgreSQL.Driver.Error

