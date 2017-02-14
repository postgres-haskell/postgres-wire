module Database.PostgreSQL.Driver.StatementStorage where

import qualified Data.HashTable.IO as H
import qualified Data.ByteString as B
import Data.ByteString.Char8 (pack)
import Data.Word (Word)
import Data.IORef

import Database.PostgreSQL.Protocol.Types

-- | Prepared statement storage
data StatementStorage = StatementStorage
    (H.BasicHashTable StatementSQL StatementName) (IORef Word)

-- | Cache policy about prepared statements.
data CachePolicy
    = AlwaysCache
    | NeverCache
    deriving (Show)

newStatementStorage :: IO StatementStorage
newStatementStorage = StatementStorage <$> H.new <*> newIORef 0

lookupStatement :: StatementStorage -> StatementSQL -> IO (Maybe StatementName)
lookupStatement (StatementStorage table _) = H.lookup table

storeStatement :: StatementStorage -> StatementSQL -> IO StatementName
storeStatement (StatementStorage table counter) stmt = do
    n <- readIORef counter
    writeIORef counter $ n + 1
    let name = StatementName . pack $ show n
    H.insert table stmt name
    pure name

getCacheSize :: StatementStorage -> IO Word
getCacheSize (StatementStorage _ counter) = readIORef counter

defaultStatementName :: StatementName
defaultStatementName = StatementName ""

