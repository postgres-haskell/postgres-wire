module Database.PostgreSQL.Driver.Error where

import Control.Exception
import Data.ByteString (ByteString)
import System.Socket (AddressInfoException)

import Database.PostgreSQL.Protocol.Types (ErrorDesc)

-- All possible exceptions:
--   SocketException
--   DecodeException.
--   IncorrectUsage.

data IncorrectUsage = IncorrectUsage

-- All possible errors.
data Error
    -- Error sended by PostgreSQL, not application error.
    = PostgresError ErrorDesc
    | AuthError AuthError
    -- Receiver errors that may occur in receiver thread. When such error occur
    -- it means that receiver thread died.
    | ReceiverError ReceiverException
    deriving (Show)

newtype ReceiverException = ReceiverException SomeException
    deriving (Show)

-- Errors that might occur at authorization phase.
-- Non-recoverable.
data AuthError
    = AuthNotSupported ByteString
    | AuthInvalidAddress
    | AuthAddressException AddressInfoException
    deriving (Show)

-- Helpers

throwErrorInIO :: Error -> IO (Either Error a)
throwErrorInIO = pure . Left

throwAuthErrorInIO :: AuthError -> IO (Either Error a)
throwAuthErrorInIO = pure . Left . AuthError

