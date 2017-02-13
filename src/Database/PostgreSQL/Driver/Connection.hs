module Database.PostgreSQL.Driver.Connection where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS(pack, unpack)
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Control.Monad
import System.Mem.Weak (Weak, deRefWeak)
import Data.Traversable
import Data.Foldable
import Control.Applicative
import Control.Exception
import GHC.Conc (labelThread)
import Data.IORef
import Data.Monoid
import Control.Concurrent (forkIOWithUnmask, killThread, ThreadId, threadDelay
                          , mkWeakThreadId)
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
-- Connection parametrized by message type in chan.
data AbsConnection mt = AbsConnection
    { connRawConnection     :: RawConnection
    , connReceiverThread    :: Weak ThreadId
    , connStatementStorage  :: StatementStorage
    , connParameters        :: ConnectionParameters
    , connOutChan           :: OutChan (Either ReceiverException mt)
    }

type Connection       = AbsConnection DataMessage
type ConnectionCommon = AbsConnection ServerMessage

type InDataChan = InChan (Either ReceiverException DataMessage)
type InAllChan  = InChan (Either ReceiverException ServerMessage)

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


type ServerMessageFilter = ServerMessage -> Bool
type NotificationHandler = Notification -> IO ()

defaultNotificationHandler :: NotificationHandler
defaultNotificationHandler = const $ pure ()

type DataDispatcher
    =  InDataChan
    -> ServerMessage
    -> [B.ByteString]
    -> IO [B.ByteString]

-- | Public
connect :: ConnectionSettings -> IO (Either Error Connection)
connect settings = connectWith settings $ \rawConn params ->
    buildConnection rawConn params
        (receiverThread rawConn)

connectCommon
    :: ConnectionSettings
    -> IO (Either Error ConnectionCommon)
connectCommon settings = connectCommon' settings defaultFilter

-- | Like 'connectCommon', but allows specify a message filter.
-- Useful for testing.
connectCommon'
    :: ConnectionSettings
    -> ServerMessageFilter
    -> IO (Either Error ConnectionCommon)
connectCommon' settings msgFilter = connectWith settings $ \rawConn params ->
    buildConnection rawConn params
        (\chan -> receiverThreadCommon rawConn chan
                    msgFilter defaultNotificationHandler)

connectWith
    :: ConnectionSettings
    -> (RawConnection -> ConnectionParameters -> IO (AbsConnection c))
    -> IO (Either Error (AbsConnection c))
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
            Left reason -> error "handle error here"
                -- TODO handle errors
                -- throwErrorInIO . DecodeError $ BS.pack reason

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
    -> (InChan (Either ReceiverException c) -> IO ())
    -> IO (AbsConnection c)
buildConnection rawConn connParams receiverAction = do
    (inChan, outChan)         <- newChan
    storage                   <- newStatementStorage

    let createReceiverThread = mask_ $ forkIOWithUnmask $ \unmask ->
            unmask (receiverAction inChan)
            `catch` (writeChan inChan . Left . ReceiverException)

    --  When receiver thread dies by any unexpected exception, than message
    --  would be written in its chan.
    createReceiverThread `bracketOnError` killThread $ \tid -> do
        labelThread tid "postgres-wire receiver"
        weakTid <- mkWeakThreadId tid

        pure AbsConnection
            { connRawConnection    = rawConn
            , connReceiverThread   = weakTid
            , connStatementStorage = storage
            , connParameters       = connParams
            , connOutChan          = outChan
            }

-- | Parses connection parameters.
parseParameters :: B.ByteString -> Either Error ConnectionParameters
parseParameters str = do
    dict <- go str HM.empty
    -- TODO handle error
    serverVersion    <- maybe (error "handle error") Right .
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
        -- TODO
        (error "handle errors") Right
        . HM.lookup key
    go str dict | B.null str = Right dict
                | otherwise = case runDecode
                    (decodeHeader >>= decodeServerMessage) str of
        Right (rest, v) -> case v of
            ParameterStatus name value -> go rest $ HM.insert name value dict
            -- messages like `BackendData` not handled
            _                          -> go rest dict
        Left reason -> error "handle error here"
            -- TODO
            -- Left . DecodeError $ BS.pack reason

handshakeTls :: RawConnection ->  IO ()
handshakeTls _ = pure ()

-- | Public
-- TODO add termination
close :: AbsConnection c -> IO ()
close conn = do
    maybe (pure ()) killThread =<< deRefWeak (connReceiverThread conn)
    rClose $ connRawConnection conn

-- | Any exception prevents thread from future work
receiverThread
    :: RawConnection
    -> InDataChan
    -> IO ()
receiverThread rawConn dataChan =
    loopExtractDataRows
        (rReceive rawConn 4096)
        (writeChan dataChan . Right)
  where
    receiveLoop
        :: Maybe Header
        -> B.ByteString
        -> [B.ByteString] -> IO ()
    -- Parsing header
    receiveLoop Nothing bs acc
        | B.length bs < 5 = do
            r <- rReceive rawConn 4096
            receiveLoop Nothing (bs <> r) acc
        | otherwise =  case runDecode decodeHeader bs of
            -- TODO handle error
            Left reason -> undefined
            -- reportReceiverError dataChan allChan
            --                 $ DecodeError $ BS.pack reason
            Right (rest, h) ->  receiveLoop (Just h) rest acc
    -- Parsing body
    receiveLoop (Just h@(Header _ len)) bs acc
        | B.length bs < len = do
            r <- rReceive rawConn 4096
            receiveLoop (Just h) (bs <> r) acc
        | otherwise = case runDecode (decodeServerMessage h) bs of
            -- TODO handle error
            Left reason -> undefined
                -- reportReceiverError dataChan allChan
                --             $ DecodeError $ BS.pack reason
            Right (rest, v) -> do
                newAcc <- dispatchExtended dataChan v acc
                receiveLoop Nothing rest newAcc

-- | Any exception prevents thread from future work
receiverThreadCommon
    :: RawConnection
    -> InAllChan
    -> ServerMessageFilter
    -> NotificationHandler
    -> IO ()
receiverThreadCommon rawConn chan msgFilter ntfHandler =
    receiveLoop Nothing ""
  where
    receiveLoop :: Maybe Header -> B.ByteString ->  IO ()
    -- Parsing header
    receiveLoop Nothing bs
        | B.length bs < 5 = do
            r <- rReceive rawConn 4096
            receiveLoop Nothing (bs <> r)
        | otherwise =  case runDecode decodeHeader bs of
            -- TODO handle error
            Left reason -> undefined
            -- reportReceiverError dataChan allChan
            --                 $ DecodeError $ BS.pack reason
            Right (rest, h) ->  receiveLoop (Just h) rest
    -- Parsing body
    receiveLoop (Just h@(Header _ len)) bs
        | B.length bs < len = do
            r <- rReceive rawConn 4096
            receiveLoop (Just h) (bs <> r)
        | otherwise = case runDecode (decodeServerMessage h) bs of
            -- TODO handle error
            Left reason -> undefined
                -- reportReceiverError dataChan allChan
                --             $ DecodeError $ BS.pack reason
            Right (rest, v) -> do
                dispatchIfNotification v ntfHandler
                when (msgFilter v) $ writeChan chan $ Right v
                receiveLoop Nothing rest
    dispatchIfNotification (NotificationResponse ntf) handler = handler ntf
    dispatchIfNotification _ _ = pure ()

-- | Dispatcher for the ExtendedQuery mode.
dispatchExtended :: DataDispatcher
dispatchExtended dataChan message acc = case message of
    -- Command is completed, return the result
    CommandComplete _ -> do
        writeChan dataChan . Right . DataMessage . DataRows . BL.fromChunks
            $ reverse acc
        pure []
    -- note that data rows go in reversed order
    DataRow row -> pure (row:acc)
    -- PostgreSQL sends this if query string was empty and datarows should be
    -- empty, but anyway we return data collected in `acc`.
    EmptyQueryResponse -> do
        writeChan dataChan . Right . DataMessage . DataRows . BL.fromChunks
            $ reverse acc
        pure []
    -- On ErrorResponse we should discard all the collected datarows.
    ErrorResponse desc -> do
        writeChan dataChan $ Right $ DataError desc
        pure []
    -- to know when command processing is finished
    ReadForQuery{}         -> do
        writeChan dataChan $ Right DataReady
        pure acc
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

sendEncode :: AbsConnection c -> Encode -> IO ()
sendEncode conn = void . rSend (connRawConnection conn) . runEncode


-- Information about connection

getServerVersion :: AbsConnection c -> ServerVersion
getServerVersion = paramServerVersion . connParameters

getServerEncoding :: AbsConnection c -> B.ByteString
getServerEncoding = paramServerEncoding . connParameters

getIntegerDatetimes :: AbsConnection c -> Bool
getIntegerDatetimes = paramIntegerDatetimes . connParameters

