module Database.PostgreSQL.Driver.StatementStorage 
    ( StatementStorage
    , CachePolicy(..)
    , newStatementStorage
    , lookupStatement
    , storeStatement
    , getCacheSize
    , defaultStatementName
    ) where

import Data.Monoid  ((<>))
import Data.IORef   (IORef, newIORef, readIORef, writeIORef)
import Data.Word    (Word)

import Data.ByteString       (ByteString)
import Data.ByteString.Char8 (pack)
import qualified Data.HashTable.IO as H

import Database.PostgreSQL.Protocol.Types

-- | Prepared statement storage
data StatementStorage = StatementStorage
    !(H.BasicHashTable StatementSQL StatementName) !(IORef Word)

-- | Cache policy about prepared statements.
data CachePolicy
    = AlwaysCache
    | NeverCache
    deriving (Show)

newStatementStorage :: IO StatementStorage
newStatementStorage = StatementStorage <$> H.new <*> newIORef 0

{-# INLINE lookupStatement #-}
lookupStatement :: StatementStorage -> StatementSQL -> IO (Maybe StatementName)
lookupStatement (StatementStorage table _) = H.lookup table

-- TODO info about exceptions and mask
{-# INLINE storeStatement #-}
storeStatement :: StatementStorage -> StatementSQL -> IO StatementName
storeStatement (StatementStorage table counter) stmt = do
    n <- readIORef counter
    writeIORef counter $ n + 1
    let name = StatementName . (statementPrefix <>) . pack $ show n
    H.insert table stmt name
    pure name

getCacheSize :: StatementStorage -> IO Word
getCacheSize (StatementStorage _ counter) = readIORef counter

defaultStatementName :: StatementName
defaultStatementName = StatementName ""

statementPrefix :: ByteString
statementPrefix = "_pw_statement_"

