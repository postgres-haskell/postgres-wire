module Database.PostgreSQL.Driver.Connection where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS(pack, unpack)
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Control.Monad
import Data.Traversable
import Data.Foldable
import Control.Applicative
import Control.Exception
import GHC.Conc (labelThread)
import Data.IORef
import Data.Monoid
import Control.Concurrent (forkIOWithUnmask, killThread, ThreadId, threadDelay)
import Data.Binary.Get ( runGetIncremental, pushChunk)
import qualified Data.Binary.Get as BG (Decoder(..))
import qualified Data.Vector as V
import Control.Concurrent.Chan.Unagi
import qualified Data.HashMap.Strict as HM
import Crypto.Hash (hash, Digest, MD5)

import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Encode (runEncode, Encode)
import Database.PostgreSQL.Protocol.Store.Decode (runDecode)

import Database.PostgreSQL.Driver.Settings
import Database.PostgreSQL.Driver.StatementStorage
import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.RawConnection

-- | Public
data Connection = Connection
    { connRawConnection     :: RawConnection
    , connReceiverThread    :: ThreadId
    -- channel only for Data messages
    , connOutDataChan       :: OutChan (Either Error DataMessage)
    -- channel for all the others messages
    , connOutAllChan        :: OutChan (Either Error ServerMessage)
    , connStatementStorage  :: StatementStorage
    , connParameters        :: ConnectionParameters
    , connMode              :: IORef ConnectionMode
    }

type InDataChan = InChan (Either Error DataMessage)
type InAllChan  = InChan (Either Error ServerMessage)

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

data ConnectionMode
    -- | In this mode, all result's data is ignored
    = SimpleQueryMode
    -- | Usual mode
    | ExtendedQueryMode

defaultConnectionMode :: ConnectionMode
defaultConnectionMode = ExtendedQueryMode

type ServerMessageFilter = ServerMessage -> Bool
type NotificationHandler = Notification -> IO ()

defaultNotificationHandler :: NotificationHandler
defaultNotificationHandler = const $ pure ()

type DataDispatcher
    =  InDataChan
    -> ServerMessage
    -> [V.Vector (Maybe B.ByteString)]
    -> IO [V.Vector (Maybe B.ByteString)]

data DataMessage = DataMessage [V.Vector (Maybe B.ByteString)]
    deriving (Show, Eq)


-- | Public
connect :: ConnectionSettings -> IO (Either Error Connection)
connect settings = connectWith settings defaultFilter

connectWith
    :: ConnectionSettings
    -> ServerMessageFilter
    -> IO (Either Error Connection)
connectWith settings msgFilter =
    bracketOnError
        (createRawConnection settings)
        (either (const $ pure ()) rClose)
        (either throwErrorInIO performAuth)
  where
    performAuth rawConn = authorize rawConn settings >>= either
            -- We should close connection on an authorization failure
            (\e -> rClose rawConn >> throwErrorInIO e)
            (\params -> Right <$> buildConnection rawConn params msgFilter)

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
        -- 4096 should be enough for the whole response from a server at
        -- the startup phase.
        resp <- rReceive rawConn 4096
        case runDecode decodeAuthResponse resp of
            Right (rest, r) -> case r of
                AuthenticationOk ->
                    pure $ parseParameters rest
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
            Left reason -> throwErrorInIO . DecodeError $ BS.pack reason

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
    -> ServerMessageFilter
    -> IO Connection
buildConnection rawConn connParams msgFilter = do
    (inDataChan, outDataChan) <- newChan
    (inAllChan, outAllChan)   <- newChan
    storage                   <- newStatementStorage
    modeRef                   <- newIORef defaultConnectionMode

    tid <- mask_ $ forkIOWithUnmask $ \unmask ->
        unmask (receiverThread msgFilter rawConn
                inDataChan inAllChan modeRef defaultNotificationHandler)
        `catch` receiverOnException inDataChan inAllChan
    labelThread tid "postgres-wire receiver"

    pure Connection
        { connRawConnection    = rawConn
        , connReceiverThread   = tid
        , connOutDataChan      = outDataChan
        , connOutAllChan       = outAllChan
        , connStatementStorage = storage
        , connParameters       = connParams
        , connMode             = modeRef
        }

-- | Parses connection parameters.
parseParameters :: B.ByteString -> Either Error ConnectionParameters
parseParameters str = do
    dict <- go str HM.empty
    serverVersion    <- maybe (Left $ DecodeError "server version") Right .
                        parseServerVersion =<< lookupKey "server_version" dict
    serverEncoding   <- lookupKey "server_encoding" dict
    integerDatetimes <- parseIntegerDatetimes <$>
                            lookupKey "integer_datetimes" dict
    pure ConnectionParameters
        { paramServerVersion    = serverVersion
        , paramIntegerDatetimes = integerDatetimes
        , paramServerEncoding   = serverEncoding
        }
  where
    lookupKey key = maybe
        (Left . DecodeError $ "Missing connection parameter " <> key ) Right
        . HM.lookup key
    go str dict | B.null str = Right dict
                | otherwise = case runDecode
                    (decodeHeader >>= decodeServerMessage) str of
        Right (rest, v) -> case v of
            ParameterStatus name value -> go rest $ HM.insert name value dict
            -- messages like `BackendData` not handled
            _                          -> go rest dict
        Left reason -> Left . DecodeError $ BS.pack reason

handshakeTls :: RawConnection ->  IO ()
handshakeTls _ = pure ()

-- | Public
-- TODO add termination
close :: Connection -> IO ()
close conn = do
    killThread $ connReceiverThread conn
    rClose $ connRawConnection conn

-- | When thread receives unexpected exception or fihishes by
-- any reason, than it writes Error to BOTH chans to prevent other threads
-- blocking on reading from these chans.
receiverThread
    :: ServerMessageFilter
    -> RawConnection
    -> InDataChan
    -> InAllChan
    -> IORef ConnectionMode
    -> NotificationHandler
    -> IO ()
receiverThread msgFilter rawConn dataChan allChan modeRef ntfHandler =
    receiveLoop Nothing "" []
  where
    receiveLoop
        :: Maybe Header
        -> B.ByteString
        -> [V.Vector (Maybe B.ByteString)] -> IO ()
    -- Parsing header
    receiveLoop Nothing bs acc
        | B.length bs < 5 = do
            r <- rReceive rawConn 4096
            receiveLoop Nothing (bs <> r) acc
        | otherwise =  case runDecode decodeHeader bs of
            Left reason -> reportReceiverError dataChan allChan
                            $ DecodeError $ BS.pack reason
            Right (rest, h) ->  receiveLoop (Just h) rest acc
    -- Parsing body
    receiveLoop (Just h@(Header _ len)) bs acc
        | B.length bs < len = do
            r <- rReceive rawConn 4096
            receiveLoop (Just h) (bs <> r) acc
        | otherwise = case runDecode (decodeServerMessage h) bs of
            Left reason -> reportReceiverError dataChan allChan
                            $ DecodeError $ BS.pack reason
            Right (rest, v) -> do
                dispatchIfNotification v ntfHandler
                when (msgFilter v) $ writeChan allChan $ Right v
                mode <- readIORef modeRef
                newAcc <- dispatch mode dataChan v acc
                receiveLoop Nothing rest newAcc

dispatchIfNotification :: ServerMessage -> NotificationHandler -> IO ()
dispatchIfNotification msg handler = case msg of
    NotificationResponse n -> handler n
    _                      -> pure ()

-- | Exception handler for receiver thread.
-- Called only in masked state.
receiverOnException :: InDataChan -> InAllChan -> SomeException -> IO ()
receiverOnException dataChan allChan exc =
    reportReceiverError dataChan allChan $ UnexpectedError exc

-- | Reporting about any unexpected error to the thread than reads from chans.
reportReceiverError :: InDataChan -> InAllChan -> Error -> IO ()
reportReceiverError dataChan allChan err = do
    writeChan dataChan (Left err)
    writeChan allChan (Left err)

dispatch :: ConnectionMode -> DataDispatcher
dispatch SimpleQueryMode   = dispatchSimple
dispatch ExtendedQueryMode = dispatchExtended

-- | Dispatcher for the SimpleQuery mode.
dispatchSimple :: DataDispatcher
dispatchSimple dataChan message = pure

-- | Dispatcher for the ExtendedQuery mode.
dispatchExtended :: DataDispatcher
dispatchExtended dataChan message acc = case message of
    -- Command is completed, return the result
    CommandComplete _ -> do
        writeChan dataChan . Right . DataMessage $ reverse acc
        pure []
    -- note that data rows go in reversed order
    DataRow row -> pure (row:acc)
    -- PostgreSQL sends this if query string was empty and datarows should be
    -- empty, but anyway we return data collected in `acc`.
    EmptyQueryResponse -> do
        writeChan dataChan . Right . DataMessage $ reverse acc
        pure []
    -- On ErrorResponse we should discard all the collected datarows.
    ErrorResponse desc -> do
        writeChan dataChan $ Left $ PostgresError desc
        pure []
    -- We does not handled `PortalSuspended` because we always send `execute`
    -- with no limit.
    -- PortalSuspended -> pure acc

    -- do nothing on other messages
    _ -> pure acc

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
    DataRow{}              -> False
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
    ReadForQuery{}         -> True
    -- as result for `describe` message
    RowDescription{}       -> True

-- Low-level sending functions

sendStartMessage :: RawConnection -> StartMessage -> IO ()
sendStartMessage rawConn msg = void $
    rSend rawConn . runEncode $ encodeStartMessage msg

-- Only for testings and simple queries
sendMessage :: RawConnection -> ClientMessage -> IO ()
sendMessage rawConn msg = void $
    rSend rawConn . runEncode $ encodeClientMessage msg

sendEncode :: RawConnection -> Encode -> IO ()
sendEncode rawConn = void . rSend rawConn . runEncode

withConnectionMode
    :: Connection -> ConnectionMode -> (Connection -> IO a) -> IO a
withConnectionMode conn mode handler = do
    let ref = connMode conn
    oldMode <- readIORef ref
    bracket_
        (atomicWriteIORef ref mode)
        (atomicWriteIORef ref oldMode)
        (handler conn)

-- Information about connection

getServerVersion :: Connection -> ServerVersion
getServerVersion = paramServerVersion . connParameters

getServerEncoding :: Connection -> B.ByteString
getServerEncoding = paramServerEncoding . connParameters

getIntegerDatetimes :: Connection -> Bool
getIntegerDatetimes = paramIntegerDatetimes . connParameters

