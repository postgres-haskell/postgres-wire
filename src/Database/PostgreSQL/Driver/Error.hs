module Database.PostgreSQL.Driver.Error where

import Data.ByteString (ByteString)
import System.Socket (AddressInfoException, SocketException)

import Database.PostgreSQL.Protocol.Types (ErrorDesc)

-- All possible errors.
data Error
    = PostgresError ErrorDesc
    | DecodeError ByteString
    | AuthError AuthError
    | SocketError SocketException
    | ImpossibleError ByteString
    deriving (Show)

-- Errors that might occur at authorization phase.
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

