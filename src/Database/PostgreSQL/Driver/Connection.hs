{-# LANGUAGE RecursiveDo #-}
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
import Data.Maybe (fromJust)
import qualified Data.Vector as V
import System.Socket hiding (connect, close, Error)
import qualified System.Socket as Socket (connect, close, send, receive)
import System.Socket.Family.Inet
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP
import System.Socket.Family.Unix
import Control.Concurrent.Chan.Unagi
import Crypto.Hash (hash, Digest, MD5)

import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types

import Database.PostgreSQL.Driver.Settings
import Database.PostgreSQL.Driver.StatementStorage
import Database.PostgreSQL.Driver.Types

data ConnectionMode
    -- | In this mode, all result's data is ignored
    = SimpleQueryMode
    -- | Usual mode
    | ExtendedQueryMode

defaultConnectionMode :: ConnectionMode
defaultConnectionMode = ExtendedQueryMode

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

type ServerMessageFilter = ServerMessage -> Bool

type NotificationHandler = Notification -> IO ()

-- All possible at errors
data Error
    = PostgresError ErrorDesc
    | ImpossibleError
    deriving (Show)

data AuthError
    = AuthPostgresError ErrorDesc
    | AuthNotSupported B.ByteString
    deriving (Show)

data DataMessage = DataMessage [V.Vector B.ByteString]
    deriving (Show)

-- | Abstraction over raw socket connection or tls connection
data RawConnection = RawConnection
    { rFlush   :: IO ()
    , rClose   :: IO ()
    , rSend    :: B.ByteString -> IO ()
    , rReceive :: Int -> IO B.ByteString
    }

defaultUnixPathDirectory :: B.ByteString
defaultUnixPathDirectory = "/var/run/postgresql"

unixPathFilename :: B.ByteString
unixPathFilename = ".s.PGSQL."

createRawConnection :: ConnectionSettings -> IO RawConnection
createRawConnection settings
        | host == ""              = unixConnection defaultUnixPathDirectory
        | "/" `B.isPrefixOf` host = unixConnection host
        | otherwise               = tcpConnection
  where
    host = settingsHost settings
    unixConnection dirPath = do
        -- 47 - `/`
        let dir = B.reverse . B.dropWhile (== 47) $ B.reverse dirPath
            path = dir <> "/" <> unixPathFilename
                   <> BS.pack (show $ settingsPort settings)
            -- TODO check for Nothing
            address = fromJust $ socketAddressUnixPath path
        s <- socket :: IO (Socket Unix Stream Unix)
        Socket.connect s address
        pure $ constructRawConnection s
    tcpConnection = do
        addressInfo <- getAddressInfo (Just host) Nothing aiV4Mapped
                        :: IO [AddressInfo Inet Stream TCP]
        let address = (socketAddress $ head addressInfo)
                        { inetPort = fromIntegral $ settingsPort settings }
                -- TODO check for empty
        s <- socket :: IO (Socket Inet Stream TCP)
        Socket.connect s address
        pure $ constructRawConnection s

constructRawConnection :: Socket f t p -> RawConnection
constructRawConnection s = RawConnection
    { rFlush = pure ()
    , rClose = Socket.close s
    , rSend = \msg -> void $ Socket.send s msg mempty
    , rReceive = \n -> Socket.receive s n mempty
    }

connect :: ConnectionSettings -> IO Connection
connect settings = connectWith settings defaultFilter

connectWith :: ConnectionSettings -> ServerMessageFilter -> IO Connection
connectWith settings msgFilter =  do
    rawConn <- createRawConnection settings
    when (settingsTls settings == RequiredTls) $ handshakeTls rawConn
    authResult <- authorize rawConn settings
    -- TODO should close connection on error
    connParams <- either (\e -> print e >> error "invalid connection")
                    pure authResult

    (inDataChan, outDataChan) <- newChan
    (inAllChan, outAllChan)   <- newChan
    storage                   <- newStatementStorage
    modeRef                   <- newIORef defaultConnectionMode

    tid <- forkIO $
        receiverThread msgFilter rawConn inDataChan inAllChan modeRef
    rec conn <- pure Connection
            { connRawConnection = rawConn
            , connReceiverThread = tid
            , connOutDataChan = outDataChan
            , connOutAllChan = outAllChan
            , connStatementStorage = storage
            , connParameters = connParams
            , connMode = modeRef
            }
    pure conn

authorize
    :: RawConnection
    -> ConnectionSettings
    -> IO (Either AuthError ConnectionParameters)
authorize rawConn settings = do
    sendStartMessage rawConn $ consStartupMessage settings
    -- 4096 should be enough for the whole response from a server at
    -- startup phase.
    r <- rReceive rawConn 4096
    case pushChunk (runGetIncremental decodeAuthResponse) r of
        BG.Done rest _ r -> case r of
            AuthenticationOk ->
                -- TODO parse parameters
                pure $ Right $ parseParameters rest
            AuthenticationCleartextPassword ->
                performPasswordAuth $ PasswordPlain $ settingsPassword settings
            AuthenticationMD5Password (MD5Salt salt) ->
                let pass = "md5" <> md5Hash (md5Hash (settingsPassword settings
                                 <> settingsUser settings) <> salt)
                in performPasswordAuth $ PasswordMD5 pass
            AuthenticationGSS         -> pure $ Left $ AuthNotSupported "GSS"
            AuthenticationSSPI        -> pure $ Left $ AuthNotSupported "SSPI"
            AuthenticationGSSContinue _ -> pure $ Left $ AuthNotSupported "GSS"
            AuthErrorResponse desc    -> pure $ Left $ AuthPostgresError desc
        -- TODO handle this case
        f -> error "athorize"
  where
    performPasswordAuth
        :: PasswordText -> IO (Either AuthError ConnectionParameters)
    performPasswordAuth password = do
        sendMessage rawConn $ PasswordMessage password
        r <- rReceive rawConn 4096
        case pushChunk (runGetIncremental decodeAuthResponse) r of
            BG.Done rest _ r -> case r of
                AuthenticationOk ->
                    pure $ Right $ parseParameters rest
                AuthErrorResponse desc ->
                    pure $ Left $ AuthPostgresError desc
                _ -> error "Impossible happened"
            -- TODO handle this case
            f -> error "authorize"
    -- TODO right parsing
    parseParameters :: B.ByteString -> ConnectionParameters
    parseParameters str = ConnectionParameters
        { paramServerVersion = ServerVersion 1 1 1
        , paramIntegerDatetimes = False
        , paramServerEncoding = ""
        }
    md5Hash :: B.ByteString -> B.ByteString
    md5Hash bs = BS.pack $ show (hash bs :: Digest MD5)

handshakeTls :: RawConnection ->  IO ()
handshakeTls _ = pure ()

close :: Connection -> IO ()
close conn = do
    killThread $ connReceiverThread conn
    rClose $ connRawConnection conn

consStartupMessage :: ConnectionSettings -> StartMessage
consStartupMessage stg = StartupMessage
    (Username $ settingsUser stg) (DatabaseName $ settingsDatabase stg)

sendStartMessage :: RawConnection -> StartMessage -> IO ()
sendStartMessage rawConn msg = void $ do
    let smsg = toStrict . toLazyByteString $ encodeStartMessage msg
    rSend rawConn smsg

sendMessage :: RawConnection -> ClientMessage -> IO ()
sendMessage rawConn msg = void $ do
    let smsg = toStrict . toLazyByteString $ encodeClientMessage msg
    rSend rawConn smsg


receiverThread
    :: ServerMessageFilter
    -> RawConnection
    -> InChan (Either Error DataMessage)
    -> InChan ServerMessage
    -> IORef ConnectionMode
    -> IO ()
receiverThread msgFilter rawConn dataChan allChan modeRef = receiveLoop []
  where
    receiveLoop :: [V.Vector B.ByteString] -> IO()
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
            newAcc <- dispatch v acc
            if B.null rest
                then pure newAcc
                else go rest newAcc
        BG.Partial _ -> error "Partial"
        BG.Fail _ _ e -> error e

    dispatch
        :: ServerMessage
        -> [V.Vector B.ByteString]
        -> IO [V.Vector B.ByteString]
    -- Command is completed, return the result
    dispatch (CommandComplete _) acc = do
        writeChan dataChan . Right . DataMessage $ reverse acc
        pure []
    -- note that data rows go in reversed order
    dispatch (DataRow row) acc = pure (row:acc)
    -- PostgreSQL sends this if query string was empty and datarows should be
    -- empty, but anyway we return data collected in `acc`.
    dispatch EmptyQueryResponse acc = do
        writeChan dataChan . Right . DataMessage $ reverse acc
        pure []
    -- On ErrorResponse we should discard all the collected datarows
    dispatch (ErrorResponse desc) acc = do
        writeChan dataChan $ Left $ PostgresError desc
        pure []
    -- TODO handle notifications
    dispatch (NotificationResponse n) acc = pure acc
    -- We does not handled this case because we always send `execute`
    -- with no limit.
    dispatch PortalSuspended acc = pure acc
    -- do nothing on other messages
    dispatch _ acc = pure acc

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

data Query = Query
    { qStatement    :: B.ByteString
    , qOids         :: V.Vector Oid
    , qValues       :: V.Vector B.ByteString
    , qParamsFormat :: Format
    , qResultFormat :: Format
    } deriving (Show)

-- | Public
sendBatch :: Connection -> [Query] -> IO ()
sendBatch conn = traverse_ sendSingle
  where
    s = connRawConnection conn
    sname = StatementName ""
    pname = PortalName ""
    sendSingle q = do
        sendMessage s $ Parse sname (StatementSQL $ qStatement q) (qOids q)
        sendMessage s $
            Bind pname sname (qParamsFormat q) (qValues q) (qResultFormat q)
        sendMessage s $ Execute pname noLimitToReceive

-- | Public
sendBatchAndSync :: Connection -> [Query] -> IO ()
sendBatchAndSync conn qs = sendBatch conn qs >> sendSync conn

-- | Public
sendBatchAndFlush :: Connection -> [Query] -> IO ()
sendBatchAndFlush conn qs = sendBatch conn qs >> sendFlush conn

-- | Public
sendSync :: Connection -> IO ()
sendSync conn = sendMessage (connRawConnection conn) Sync

-- | Public
sendFlush :: Connection -> IO ()
sendFlush conn = sendMessage (connRawConnection conn) Flush

-- | Public
readNextData :: Connection -> IO (Either Error DataMessage)
readNextData conn = readChan $ connOutDataChan conn

-- | Public
sendSimpleQuery :: Connection -> B.ByteString -> IO (Either Error ())
sendSimpleQuery conn q = withConnectionMode conn SimpleQueryMode $ \c -> do
    sendMessage (connRawConnection c) $ SimpleQuery (StatementSQL q)
    readReadyForQuery c

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

-- | Public
-- SHOULD BE called after every sended `Sync` message
-- skips all messages except `ReadyForQuery`
readReadyForQuery :: Connection -> IO (Either Error ())
readReadyForQuery = fmap (liftError . findFirstError)
                    . collectBeforeReadyForQuery
  where
    liftError = maybe (Right ()) (Left . PostgresError)

findFirstError :: [ServerMessage] -> Maybe ErrorDesc
findFirstError []                       = Nothing
findFirstError (ErrorResponse desc : _) = Just desc
findFirstError (_ : xs)                 = findFirstError xs

-- Collects all messages received before ReadyForQuery
collectBeforeReadyForQuery :: Connection -> IO [ServerMessage]
collectBeforeReadyForQuery conn = do
    msg <- readChan $ connOutAllChan conn
    case msg of
        ReadForQuery{} -> pure []
        m              -> (m:) <$> collectBeforeReadyForQuery conn

-- | Public
describeStatement
    :: Connection
    -> StatementSQL
    -> IO (Either Error (V.Vector Oid, V.Vector FieldDescription))
describeStatement conn stmt = do
    sendMessage s $ Parse sname stmt []
    sendMessage s $ DescribeStatement sname
    sendMessage s Sync
    parseMessages <$> collectBeforeReadyForQuery conn
  where
    s = connRawConnection conn
    sname = StatementName ""
    parseMessages msgs = case msgs of
        [ParameterDescription params, NoData]
            -> Right (params, [])
        [ParameterDescription params, RowDescription fields]
            -> Right (params, fields)
        xs  -> maybe (error "Impossible happened") (Left . PostgresError )
               $ findFirstError xs

