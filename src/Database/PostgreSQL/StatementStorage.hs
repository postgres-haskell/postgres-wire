module Database.PostgreSQL.StatementStorage where

import qualified Data.HashTable.IO as H
import qualified Data.ByteString as B
import Data.ByteString.Char8 (pack)
import Data.Word (Word)
import Data.IORef

import Database.PostgreSQL.Protocol.Types

-- | Prepared statement storage
data StatementStorage = StatementStorage
    (H.CuckooHashTable StatementSQL StatementName) (IORef Word)

newStatementStorage :: IO StatementStorage
newStatementStorage = StatementStorage <$> H.new <*> newIORef 0

lookupStatement :: StatementStorage -> StatementSQL -> IO (Maybe StatementName)
lookupStatement (StatementStorage table _) = H.lookup table

storageStatement :: StatementStorage -> StatementSQL -> IO StatementName
storageStatement (StatementStorage table counter) stmt = do
    n <- readIORef counter
    writeIORef counter $ n + 1
    let name = StatementName . pack $ show n
    H.insert table stmt name
    pure name

