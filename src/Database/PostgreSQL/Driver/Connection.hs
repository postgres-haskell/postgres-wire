module Database.PostgreSQL.Driver.Connection where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS(pack)
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Control.Monad
import Data.Traversable
import Data.Foldable
import Control.Applicative
import Data.IORef
import Data.Monoid
import Control.Concurrent (forkIO, killThread, ThreadId, threadDelay)
import Data.Binary.Get ( runGetIncremental, pushChunk)
import qualified Data.Binary.Get as BG (Decoder(..))
import qualified Data.Vector as V
import Control.Concurrent.Chan.Unagi
import Crypto.Hash (hash, Digest, MD5)

import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types

import Database.PostgreSQL.Driver.Settings
import Database.PostgreSQL.Driver.StatementStorage
import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.RawConnection

data ConnectionMode
    -- | In this mode, all result's data is ignored
    = SimpleQueryMode
    -- | Usual mode
    | ExtendedQueryMode

defaultConnectionMode :: ConnectionMode
defaultConnectionMode = ExtendedQueryMode

type ServerMessageFilter = ServerMessage -> Bool
type NotificationHandler = Notification -> IO ()

data DataMessage = DataMessage [V.Vector B.ByteString]
    deriving (Show, Eq)

-- | Parameters of the current connection.
-- We store only the parameters that cannot change after startup.
-- For more information about additional parameters see documentation.
data ConnectionParameters = ConnectionParameters
    { paramServerVersion    :: ServerVersion
    , paramServerEncoding   :: B.ByteString   -- ^ character set name
    , paramIntegerDatetimes :: Bool         -- ^ True if integer datetimes used
    } deriving (Show)

-- | Public
data Connection = Connection
    { connRawConnection      :: RawConnection
    , connReceiverThread     :: ThreadId
    -- channel only for Data messages
    , connOutDataChan        :: OutChan (Either Error DataMessage)
    -- channel for all the others messages
    , connOutAllChan         :: OutChan ServerMessage
    , connStatementStorage   :: StatementStorage
    , connParameters         :: ConnectionParameters
    , connMode               :: IORef ConnectionMode
    }

-- | Public
connect :: ConnectionSettings -> IO (Either Error Connection)
connect settings = connectWith settings defaultFilter

connectWith
    :: ConnectionSettings
    -> ServerMessageFilter
    -> IO (Either Error Connection)
connectWith settings msgFilter =
    createRawConnection settings >>=
        either throwErrorInIO (\rawConn ->
            authorize rawConn settings >>=
                either throwErrorInIO (\params ->
                    Right <$> buildConnection rawConn params msgFilter))

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

    tid <- forkIO $
        receiverThread msgFilter rawConn inDataChan inAllChan modeRef
    pure Connection
        { connRawConnection = rawConn
        , connReceiverThread = tid
        , connOutDataChan = outDataChan
        , connOutAllChan = outAllChan
        , connStatementStorage = storage
        , connParameters = connParams
        , connMode = modeRef
        }

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
        r <- rReceive rawConn 4096
        case pushChunk (runGetIncremental decodeAuthResponse) r of
            BG.Done rest _ r -> case r of
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
            -- TODO handle this case
            -- data receiving error
            f -> error "athorize"

    performPasswordAuth password = do
        sendMessage rawConn $ PasswordMessage password
        readAuthResponse

    makePlainPassword = PasswordPlain $ settingsPassword settings
    makeMd5Password salt = PasswordMD5 $
        "md5" <> md5Hash (md5Hash
            (settingsPassword settings <> settingsUser settings) <> salt)
    md5Hash bs = BS.pack $ show (hash bs :: Digest MD5)

-- TODO right parsing
-- | Parses connection parameters.
parseParameters :: B.ByteString -> Either Error ConnectionParameters
parseParameters str = Right ConnectionParameters
    { paramServerVersion = ServerVersion 1 1 1
    , paramIntegerDatetimes = False
    , paramServerEncoding = ""
    }

handshakeTls :: RawConnection ->  IO ()
handshakeTls _ = pure ()

-- | Public
close :: Connection -> IO ()
close conn = do
    killThread $ connReceiverThread conn
    rClose $ connRawConnection conn

receiverThread
    :: ServerMessageFilter
    -> RawConnection
    -> InChan (Either Error DataMessage)
    -> InChan ServerMessage
    -> IORef ConnectionMode
    -> IO ()
receiverThread msgFilter rawConn dataChan allChan modeRef = receiveLoop []
  where
    receiveLoop :: [V.Vector B.ByteString] -> IO ()
    receiveLoop acc = do
        r <- rReceive rawConn 4096
        -- print r
        go r acc >>= receiveLoop

    decoder = runGetIncremental decodeServerMessage
    go :: B.ByteString -> [V.Vector B.ByteString] -> IO [V.Vector B.ByteString]
    go str acc = case pushChunk decoder str of
        BG.Done rest _ v -> do
            -- putStrLn $ "Received: " ++ show v
            when (msgFilter v) $ writeChan allChan v
            newAcc <- dispatch dataChan v acc
            if B.null rest
                then pure newAcc
                else go rest newAcc
        -- TODO right parsing
        BG.Partial _ -> error "Partial"
        BG.Fail _ _ e -> error e

dispatch
    :: InChan (Either Error DataMessage)
    -> ServerMessage
    -> [V.Vector B.ByteString]
    -> IO [V.Vector B.ByteString]
-- Command is completed, return the result
dispatch dataChan (CommandComplete _) acc = do
    writeChan dataChan . Right . DataMessage $ reverse acc
    pure []
-- note that data rows go in reversed order
dispatch dataChan (DataRow row) acc = pure (row:acc)
-- PostgreSQL sends this if query string was empty and datarows should be
-- empty, but anyway we return data collected in `acc`.
dispatch dataChan EmptyQueryResponse acc = do
    writeChan dataChan . Right . DataMessage $ reverse acc
    pure []
-- On ErrorResponse we should discard all the collected datarows
dispatch dataChan (ErrorResponse desc) acc = do
    writeChan dataChan $ Left $ PostgresError desc
    pure []
-- TODO handle notifications
dispatch dataChan (NotificationResponse n) acc = pure acc
-- We does not handled this case because we always send `execute`
-- with no limit.
dispatch dataChan PortalSuspended acc = pure acc
-- do nothing on other messages
dispatch dataChan _ acc = pure acc

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
sendStartMessage rawConn msg = void $ do
    let smsg = toStrict . toLazyByteString $ encodeStartMessage msg
    rSend rawConn smsg

sendMessage :: RawConnection -> ClientMessage -> IO ()
sendMessage rawConn msg = void $ do
    let smsg = toStrict . toLazyByteString $ encodeClientMessage msg
    rSend rawConn smsg

withConnectionMode
    :: Connection -> ConnectionMode -> (Connection -> IO a) -> IO a
withConnectionMode conn mode handler = do
    oldMode <- readIORef ref
    atomicWriteIORef ref mode
    r <- handler conn
    atomicWriteIORef ref oldMode
    pure r
  where
    ref = connMode conn

