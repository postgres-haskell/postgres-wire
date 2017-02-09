module Database.PostgreSQL.Driver.Error where

import Control.Exception
import Data.ByteString (ByteString)
import System.Socket (AddressInfoException)

import Database.PostgreSQL.Protocol.Types (ErrorDesc)

-- All possible errors.
data Error
    = PostgresError ErrorDesc
    | DecodeError ByteString
    | AuthError AuthError
    | ImpossibleError ByteString
    | UnexpectedError SomeException
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

