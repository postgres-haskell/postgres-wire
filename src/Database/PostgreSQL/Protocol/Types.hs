module Database.PostgreSQL.Protocol.Types where

import Data.Word (Word32, Word8)
import Data.Int (Int32, Int16)
import Data.Hashable (Hashable)
import Data.ByteString (ByteString)
import Data.Vector (Vector)

-- Common
newtype Oid           = Oid { unOid :: Int32 }    deriving (Show)
newtype StatementName = StatementName ByteString  deriving (Show)
newtype StatementSQL  = StatementSQL ByteString   deriving (Show, Eq, Hashable)
newtype PortalName    = PortalName ByteString     deriving (Show)
newtype ChannelName   = ChannelName ByteString    deriving (Show)

-- Startup phase
newtype Username     = Username ByteString     deriving (Show)
newtype DatabaseName = DatabaseName ByteString deriving (Show)
newtype MD5Salt      = MD5Salt ByteString      deriving (Show)

data PasswordText
    = PasswordPlain ByteString
    | PasswordMD5 ByteString
    deriving (Show)

newtype ServerProcessId = ServerProcessId Int32 deriving (Show)
newtype ServerSecretKey  = ServerSecretKey Int32 deriving (Show)

newtype RowsCount = RowsCount Word deriving (Show)

-- | Maximum number of rows to return, if portal contains a query that
-- returns rows (ignored otherwise). Zero denotes "no limit".
newtype RowsToReceive = RowsToReceive Int32 deriving (Show)

-- | Query will returned unlimited rows.
noLimitToReceive :: RowsToReceive
noLimitToReceive = RowsToReceive 0

-- | Information about completed command.
data CommandResult
    --  oid is the object ID of the inserted row if rows is 1 and
    --  the target table has OIDs; otherwise oid is 0.
    = InsertCompleted Oid RowsCount
    | DeleteCompleted RowsCount
    | UpdateCompleted RowsCount
    | SelectCompleted RowsCount
    | MoveCompleted   RowsCount
    | FetchCompleted  RowsCount
    | CopyCompleted   RowsCount
    deriving (Show)

-- | Parameters of the current connection.
-- We store only the parameters that cannot change after startup.
-- For more information about additional parameters see documentation.
data ConnectionParameters = ConnectionParameters
    { paramServerVersion    :: ServerVersion
    , paramServerEncoding   :: ByteString   -- ^ character set name
    , paramIntegerDatetimes :: Bool         -- ^ True if integer datetimes used
    } deriving (Show)

-- | Server version contains major, minor, revision numbers.
data ServerVersion = ServerVersion Word8 Word8 Word8

instance Show ServerVersion where
    show (ServerVersion major minor revision) =
        show major ++ "." ++ show minor ++ "." ++ show revision

data TransactionStatus
    -- | not in a transaction block
    = TransactionIdle
    -- | in a transaction block
    | TransactionInBlock
    -- | in a failed transaction block
    -- (queries will be rejected until block is ended)
    | TransactionFailed
    deriving (Show)

data Format = Text | Binary
    deriving (Show)

-- All the commands have the same names as presented in the official
-- postgres documentation except explicit exclusions.
--
data StartMessage
    = StartupMessage Username DatabaseName
    | SSLRequest
    deriving (Show)

data AuthResponse
    = AuthenticationOk
    | AuthenticationCleartextPassword
    | AuthenticationMD5Password MD5Salt
    | AuthenticationGSS
    | AuthenticationSSPI
    | AuthenticationGSSContinue ByteString
    deriving (Show)

data ClientMessage
    = Bind PortalName StatementName
        Format                  -- parameter format code, one format for all
        (Vector ByteString) -- the values of parameters
        Format                  -- to apply code to all result columns
    -- Postgres use one command `close` for closing both statements and
    -- portals, but we distinguish them
    | CloseStatement StatementName
    | ClosePortal PortalName
    -- Postgres use one command `describe` for describing both statements
    -- and portals, but we distinguish them
    | DescribeStatement StatementName
    | DescribePortal PortalName
    | Execute PortalName RowsToReceive
    | Flush
    | Parse StatementName StatementSQL (Vector Oid)
    | PasswordMessage PasswordText
    -- PostgreSQL names it `Query`
    | SimpleQuery StatementSQL
    | Sync
    | Terminate
    deriving (Show)

data CancelRequest = CancelRequest ServerProcessId ServerSecretKey
    deriving (Show)

data ServerMessage
    = BackendKeyData ServerProcessId ServerSecretKey
    | BindComplete
    | CloseComplete
    | CommandComplete CommandResult
    | DataRow (Vector ByteString)
    | EmptyQueryResponse
    | ErrorResponse ErrorDesc
    | NoData
    | NoticeResponse NoticeDesc
    | NotificationResponse Notification
    | ParameterDescription (Vector Oid)
    | ParameterStatus ByteString ByteString -- name and value
    | ParseComplete
    | PortalSuspended
    | ReadForQuery TransactionStatus
    | RowDescription (Vector FieldDescription)
    deriving (Show)

data Notification = Notification
    { notificationProcessId :: ServerProcessId
    , notificationChannel   :: ChannelName
    , notificationPayload   :: ByteString
    } deriving (Show)

data FieldDescription = FieldDescription {
    -- | the field name
      fieldName :: ByteString
    -- | If the field can be identified as a column of a specific table,
    -- the object ID of the table; otherwise zero.
    , fieldTableOid :: Oid
    --  | If the field can be identified as a column of a specific table,
    --  the attribute number of the column; otherwise zero.
    , fieldColumnNumber :: Int16
    -- | The object ID of the field's data type.
    , fieldTypeOid :: Oid
    -- | The data type size (see pg_type.typlen). Note that negative
    -- values denote variable-width types.
    , fieldSize :: Int16
    -- | The type modifier (see pg_attribute.atttypmod).
    , fieldMode :: Int32
    -- | The format code being used for the field. In a RowDescription
    -- returned from the statement variant of Describe, the format code
    -- is not yet known and will always be zero.
    , fieldFormat :: Format
    } deriving (Show)

data ErrorSeverity
    = SeverityError
    | SeverityFatal
    | SeverityPanic
    | UnknownErrorSeverity
    deriving (Show, Eq)

data NoticeSeverity
    = SeverityWarning
    | SeverityNotice
    | SeverityDebug
    | SeverityInfo
    | SeverityLog
    | UnknownNoticeSeverity
    deriving (Show, Eq)

data ErrorDesc = ErrorDesc
    { errorSeverity         :: ErrorSeverity
    , errorCode             :: ByteString
    , errorMessage          :: ByteString
    , errorDetail           :: Maybe ByteString
    , errorHint             :: Maybe ByteString
    , errorPosition         :: Maybe Int
    , errorInternalPosition :: Maybe Int
    , errorInternalQuery    :: Maybe ByteString
    , errorContext          :: Maybe ByteString
    , errorSchema           :: Maybe ByteString
    , errorTable            :: Maybe ByteString
    , errorColumn           :: Maybe ByteString
    , errorDataType         :: Maybe ByteString
    , errorConstraint       :: Maybe ByteString
    , errorSourceFilename   :: Maybe ByteString
    , errorSourceLine       :: Maybe Int
    , errorSourceRoutine    :: Maybe ByteString
    } deriving (Show)

data NoticeDesc = NoticeDesc
    { noticeSeverity         :: NoticeSeverity
    , noticeCode             :: ByteString
    , noticeMessage          :: ByteString
    , noticeDetail           :: Maybe ByteString
    , noticeHint             :: Maybe ByteString
    , noticePosition         :: Maybe Int
    , noticeInternalPosition :: Maybe Int
    , noticeInternalQuery    :: Maybe ByteString
    , noticeContext          :: Maybe ByteString
    , noticeSchema           :: Maybe ByteString
    , noticeTable            :: Maybe ByteString
    , noticeColumn           :: Maybe ByteString
    , noticeDataType         :: Maybe ByteString
    , noticeConstraint       :: Maybe ByteString
    , noticeSourceFilename   :: Maybe ByteString
    , noticeSourceLine       :: Maybe Int
    , noticeSourceRoutine    :: Maybe ByteString
    } deriving (Show)

-- TODO
-- * COPY subprotocol commands
-- * function call, is deprecated by postgres
-- * AuthenticationKerberosV5 IS deprecated by postgres
-- * AuthenticationSCMCredential IS deprecated since postgres 9.1
-- * NOTICE bind command can have different formats for parameters and results
--   but we assume that there will be one format for all.
-- * We dont store parameters of connection that may change after startup

