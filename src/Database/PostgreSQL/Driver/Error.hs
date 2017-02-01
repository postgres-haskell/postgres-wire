module Database.PostgreSQL.Driver.Error where

import Data.ByteString (ByteString)

import Database.PostgreSQL.Protocol.Types (ErrorDesc)

-- All possible errors.
data Error
    = PostgresError ErrorDesc
    | AuthError AuthError
    | ImpossibleError ByteString
    deriving (Show)

-- Errors that might occur at authorization phase.
data AuthError
    = AuthNotSupported ByteString
    | AuthInvalidAddress
    deriving (Show)

-- Helpers

throwErrorInIO :: Error -> IO (Either Error a)
throwErrorInIO = pure . Left

throwAuthErrorInIO :: AuthError -> IO (Either Error a)
throwAuthErrorInIO = pure . Left . AuthError

