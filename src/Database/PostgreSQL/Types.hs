{-
  * We dont store parameters of connection that may change after startup
-}
module Database.PostgreSQL.Types where

import Data.ByteString (ByteString)

import Database.PostgreSQL.Protocol.Types

-- | Parameters of the current connection.
-- We store only the parameters that cannot change after startup.
-- For more information about additional parameters see documentation.
data ConnectionParameters = ConnectionParameters
    { paramServerVersion    :: ServerVersion
    , paramServerEncoding   :: ByteString   -- ^ character set name
    , paramIntegerDatetimes :: Bool         -- ^ True if integer datetimes used
    } deriving (Show)

