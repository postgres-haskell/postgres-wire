module Database.PostgreSQL.Driver.Error 
    (
    -- * Errors
      Error(..)
    , AuthError(..)
    -- * Exceptions
    , ReceiverException(..)
    , IncorrectUsage
    , ProtocolException
    , PeerClosedConnection
    -- * helpers
    , throwIncorrectUsage
    , throwProtocolEx
    , throwClosedException
    , eitherToProtocolEx
    , throwErrorInIO
    , throwAuthErrorInIO
    ) where

import Control.Exception (throwIO, Exception(..), SomeException)
import Data.ByteString (ByteString)
import System.Socket (AddressInfoException)
import qualified Data.ByteString.Char8 as BS

import Database.PostgreSQL.Protocol.Types (ErrorDesc)

-- All possible errors.
data Error
    -- Error sended by PostgreSQL, not application error.
    = PostgresError ErrorDesc
    | AuthError AuthError
    -- Receiver errors that may occur in receiver thread. 
    -- When such error occurs it means that receiver thread died.
    | ReceiverError ReceiverException
    deriving (Show)

-- | Unexcepted exception in the ReceiverThread.
newtype ReceiverException = ReceiverException SomeException
    deriving (Show)

-- Errors that might occur at authorization phase.
-- Non-recoverable.
data AuthError
    = AuthNotSupported ByteString
    | AuthInvalidAddress
    | AuthAddressException AddressInfoException
    deriving (Show)

-- | Exception throwing when `readNextData` or `waitReadyForQuery`
-- used incorrectly.
newtype IncorrectUsage = IncorrectUsage ByteString
    deriving (Show)

instance Exception IncorrectUsage where
    displayException (IncorrectUsage msg) =
        "Incorrect usage: " ++ BS.unpack msg

-- | Exception in high-level parsing protocol messages.
newtype ProtocolException = ProtocolException ByteString
    deriving (Show)

instance Exception ProtocolException where
    displayException (ProtocolException msg) =
        "Exception in protocol, " ++ BS.unpack msg

-- | Exception throw when remote peer closes connections.
data PeerClosedConnection = PeerClosedConnection 
    deriving (Show)

instance Exception PeerClosedConnection where
    displayException _ = "Remote peer closed the connection"

throwIncorrectUsage :: ByteString -> IO a
throwIncorrectUsage = throwIO . IncorrectUsage

throwProtocolEx :: ByteString -> IO a
throwProtocolEx = throwIO . ProtocolException

throwClosedException :: IO a
throwClosedException = throwIO PeerClosedConnection

eitherToProtocolEx :: Either ByteString a -> IO a
eitherToProtocolEx = either throwProtocolEx pure

-- TODO rename without throw since it actually does not throw exceptions
throwErrorInIO :: Error -> IO (Either Error a)
throwErrorInIO = pure . Left

-- TODO rename without throw since it actually does not throw exceptions
throwAuthErrorInIO :: AuthError -> IO (Either Error a)
throwAuthErrorInIO = pure . Left . AuthError

