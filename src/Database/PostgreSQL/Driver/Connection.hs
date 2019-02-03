module Database.PostgreSQL.Driver.Connection 
    ( -- * Connection types
      Connection(..)
    , ServerMessageFilter
    , NotificationHandler
    -- * Connection parameters
    , getServerVersion
    , getServerEncoding
    , getIntegerDatetimes
    -- * Work with connection
    , connect
    , connect'
    , sendStartMessage
    , sendMessage
    , sendEncode
    , close
    -- * Useful for testing
    , defaultNotificationHandler
    , filterAllowedAll
    , defaultFilter
    ) where

import Data.Monoid                   ((<>))
import Control.Concurrent            (forkIOWithUnmask, killThread, ThreadId, 
                                      threadDelay , mkWeakThreadId)
import Control.Concurrent.STM        (atomically)
import Control.Concurrent.STM.TQueue (TQueue, writeTQueue, newTQueueIO)
import Control.Exception             (SomeException, bracketOnError, catch, 
                                      mask_, catch, throwIO)
import Control.Monad                 (void, when)
import GHC.Conc                      (labelThread)
import System.Mem.Weak               (Weak, deRefWeak)

import Crypto.Hash                   (hash, Digest, MD5)
import System.Socket                 (eBadFileDescriptor)
import qualified Data.HashMap.Strict as HM
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS(pack, unpack)

import Database.PostgreSQL.Protocol.DataRows
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Parsers
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Encode (runEncode, Encode)
import Database.PostgreSQL.Protocol.Store.Decode (runDecode)

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.Settings
import Database.PostgreSQL.Driver.StatementStorage
import Database.PostgreSQL.Driver.RawConnection

type InChan = TQueue (Either ReceiverException ServerMessage)

-- | Public
-- Connection parametrized by message type in chan.
data Connection = Connection
    { connRawConnection     :: !RawConnection
    , connReceiverThread    :: !(Weak ThreadId)
    , connStatementStorage  :: !StatementStorage
    , connParameters        :: !ConnectionParameters
    , connChan              :: !InChan
    }


type ServerMessageFilter = ServerMessage -> Bool
type NotificationHandler = Notification -> IO ()

-- | Parameters of the current connection.
-- We store only the parameters that cannot change after startup.
-- For more information about additional parameters see
-- PostgreSQL documentation.
data ConnectionParameters = ConnectionParameters
    { paramServerVersion    :: ServerVersion
    -- | character set name
    , paramServerEncoding   :: B.ByteString
    -- | True if integer datetimes used
    , paramIntegerDatetimes :: Bool
    } deriving (Show)

-- Getting information about connection

-- | Returns a server version of the current connection.
getServerVersion :: Connection -> ServerVersion
getServerVersion = paramServerVersion . connParameters

-- | Returns a server encoding of the current connection.
getServerEncoding :: Connection -> B.ByteString
getServerEncoding = paramServerEncoding . connParameters

-- | Returns whether server uses integer datetimes.
getIntegerDatetimes :: Connection -> Bool
getIntegerDatetimes = paramIntegerDatetimes . connParameters

-- | Public
connect
    :: ConnectionSettings
    -> IO (Either Error Connection)
connect settings = connect' settings defaultFilter

-- | Like 'connect', but allows specify a message filter.
-- Useful for testing.
connect'
    :: ConnectionSettings
    -> ServerMessageFilter
    -> IO (Either Error Connection)
connect' settings msgFilter = connectWith settings $ \rawConn params ->
    buildConnection rawConn params
        (\chan -> receiverThread rawConn chan
                    msgFilter defaultNotificationHandler)

-- Low-level sending functions

{-# INLINE sendStartMessage #-}
sendStartMessage :: RawConnection -> StartMessage -> IO ()
sendStartMessage rawConn msg = void $
    rSend rawConn . runEncode $ encodeStartMessage msg

-- Only for testings and simple queries
{-# INLINE sendMessage #-}
sendMessage :: RawConnection -> ClientMessage -> IO ()
sendMessage rawConn msg = void $
    rSend rawConn . runEncode $ encodeClientMessage msg

{-# INLINE sendEncode #-}
sendEncode :: Connection -> Encode -> IO ()
sendEncode conn = void . rSend (connRawConnection conn) . runEncode

connectWith
    :: ConnectionSettings
    -> (RawConnection -> ConnectionParameters -> IO Connection)
    -> IO (Either Error Connection)
connectWith settings buildAction =
    bracketOnError
        (createRawConnection settings)
        (either (const $ pure ()) rClose)
        (either throwErrorInIO performAuth)
  where
    performAuth rawConn = authorize rawConn settings >>= either
            -- We should close connection on an authorization failure
            (\e -> rClose rawConn >> throwErrorInIO e)
            (\params -> Right <$> buildAction rawConn params)

-- | Authorizes on the server and reads connection parameters.
authorize
    :: RawConnection
    -> ConnectionSettings
    -> IO (Either Error ConnectionParameters)
authorize rawConn settings = do
    sendStartMessage rawConn $ StartupMessage
        (Username $ settingsUser settings)
        (DatabaseName $ settingsDatabase settings)
    readAuthResponse
  where
    readAuthResponse = do
        -- 1024 should be enough for the auth response from a server at
        -- the startup phase.
        resp <- rReceive rawConn mempty 1024
        case runDecode decodeAuthResponse resp of
            (rest, r) -> case r of
                AuthenticationOk ->
                    parseParameters
                        (\bs ->  rReceive rawConn bs 1024) rest
                AuthenticationCleartextPassword ->
                    performPasswordAuth makePlainPassword
                AuthenticationMD5Password (MD5Salt salt) ->
                    performPasswordAuth $ makeMd5Password salt
                AuthenticationGSS         ->
                    throwAuthErrorInIO $ AuthNotSupported "GSS"
                AuthenticationSSPI        ->
                    throwAuthErrorInIO $ AuthNotSupported "SSPI"
                AuthenticationGSSContinue _ ->
                    throwAuthErrorInIO $ AuthNotSupported "GSS"
                AuthErrorResponse desc    ->
                    throwErrorInIO $ PostgresError desc

    performPasswordAuth password = do
        sendMessage rawConn $ PasswordMessage password
        readAuthResponse

    makePlainPassword = PasswordPlain $ settingsPassword settings
    makeMd5Password salt = PasswordMD5 $
        "md5" <> md5Hash (md5Hash
            (settingsPassword settings <> settingsUser settings) <> salt)
    md5Hash bs = BS.pack $ show (hash bs :: Digest MD5)

buildConnection
    :: RawConnection
    -> ConnectionParameters
    -- action in receiver thread
    -> (InChan -> IO ())
    -> IO Connection
buildConnection rawConn connParams receiverAction = do
    chan    <- newTQueueIO
    storage <- newStatementStorage

    let createReceiverThread = mask_ $ forkIOWithUnmask $ \unmask ->
            unmask (receiverAction chan)
            `catch` (writeChan chan . Left . ReceiverException)

    --  When receiver thread dies by any unexpected exception, than message
    --  would be written in its chan.
    createReceiverThread `bracketOnError` killThread $ \tid -> do
        labelThread tid "postgres-wire receiver"
        weakTid <- mkWeakThreadId tid

        pure Connection
            { connRawConnection    = rawConn
            , connReceiverThread   = weakTid
            , connStatementStorage = storage
            , connParameters       = connParams
            , connChan          = chan
            }

-- | Parses connection parameters.
parseParameters 
    :: (B.ByteString -> IO B.ByteString)
    -> B.ByteString 
    -> IO (Either Error ConnectionParameters)
parseParameters action str = Right <$> do
    dict <- parseDict str HM.empty
    serverVersion    <- eitherToProtocolEx  .  parseServerVersion =<<
                            lookupKey "server_version" dict
    serverEncoding   <- lookupKey "server_encoding" dict
    integerDatetimes <- eitherToProtocolEx  . parseIntegerDatetimes =<<
                            lookupKey "integer_datetimes" dict
    pure  ConnectionParameters
        { paramServerVersion    = serverVersion
        , paramIntegerDatetimes = integerDatetimes
        , paramServerEncoding   = serverEncoding
        }
  where
    parseDict bs dict = do
        (rest, v) <- decodeNextServerMessage bs action
        case v of
            ParameterStatus name value
                -> parseDict rest $ HM.insert name value dict
            ReadyForQuery _ -> pure dict
            _ -> parseDict rest dict

    lookupKey key = maybe
        (throwProtocolEx $ "Required parameter status missing: " <> key)
        pure . HM.lookup key

handshakeTls :: RawConnection ->  IO ()
handshakeTls _ = pure ()

-- | Closes connection. Does not throw exceptions when socket is closed.
close :: Connection -> IO ()
close conn = do
    maybe (pure ()) killThread =<< deRefWeak (connReceiverThread conn)
    sendMessage (connRawConnection conn) Terminate `catch` handlerEx
    rClose $ connRawConnection conn
  where
    handlerEx e | e == eBadFileDescriptor = pure ()
                | otherwise               = throwIO e

-- | Any exception prevents thread from future work.
receiverThread
    :: RawConnection
    -> InChan
    -> ServerMessageFilter
    -> NotificationHandler
    -> IO ()
receiverThread rawConn chan msgFilter ntfHandler = go ""
  where
    go bs = do
        (rest, msg) <- decodeNextServerMessage bs readMoreAction
        handler msg >> go rest

    readMoreAction bs = rReceive rawConn bs 4096
    handler msg = do
        dispatchIfNotification msg ntfHandler
        when (msgFilter msg) $ writeChan chan $ Right msg

    dispatchIfNotification (NotificationResponse ntf) handler = handler ntf
    dispatchIfNotification _ _ = pure ()

-- | Helper to read from queue.
{-# INLINE writeChan #-}
writeChan :: TQueue a -> a -> IO ()
writeChan q = atomically . writeTQueue q

defaultNotificationHandler :: NotificationHandler
defaultNotificationHandler = const $ pure ()

-- | For testings purposes.
filterAllowedAll :: ServerMessageFilter
filterAllowedAll _ = True

defaultFilter :: ServerMessageFilter
defaultFilter msg = case msg of
    -- PostgreSQL sends it only in startup phase
    BackendKeyData{}       -> False
    -- just ignore
    BindComplete           -> False
    -- just ignore
    CloseComplete          -> False
    -- messages affecting data handled in dispatcher
    CommandComplete{}      -> False
    -- messages affecting data handled in dispatcher
    DataRow{}              -> True
    -- messages affecting data handled in dispatcher
    EmptyQueryResponse     -> False
    -- We need collect all errors to know whether the whole command is successful
    ErrorResponse{}        -> True
    -- We need to know if the server send NoData on `describe` message
    NoData                 -> True
    -- All notices are not showing
    NoticeResponse{}       -> False
    -- notifications will be handled by callbacks or in a separate channel
    NotificationResponse{} -> False
    -- As result for `describe` message
    ParameterDescription{} -> True
    -- we dont store any run-time parameter that is not a constant
    ParameterStatus{}      -> False
    -- just ignore
    ParseComplete          -> False
    -- messages affecting data handled in dispatcher
    PortalSuspended        -> False
    -- to know when command processing is finished
    ReadyForQuery{}         -> True
    -- as result for `describe` message
    RowDescription{}       -> True

