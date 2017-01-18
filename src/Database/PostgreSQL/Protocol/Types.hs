module Database.PostgreSQL.Protocol.Types where

import Data.Word (Word32, Word8)
import Data.Int (Int32)
import qualified Data.ByteString as B
import qualified Data.Vector as V

-- Common
newtype Oid           = Oid Int32                   deriving (Show)
newtype StatementName = StatementName B.ByteString  deriving (Show)
newtype StatementSQL  = StatementSQL B.ByteString   deriving (Show)
newtype PortalName    = PortalName B.ByteString     deriving (Show)
newtype ChannelName   = ChannelName B.ByteString    deriving (Show)

-- Startup phase
newtype Username     = Username B.ByteString     deriving (Show)
newtype DatabaseName = DatabaseName B.ByteString deriving (Show)
newtype PasswordText = PasswordText B.ByteString deriving (Show)
newtype MD5Salt      = MD5Salt Word32            deriving (Show)

newtype ServerProccessId = ServerProcessId Int32 deriving (Show)
newtype ServerSecretKey  = ServerSecrecKey Int32 deriving (Show)

newtype RowsCount = RowsCount Word

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

-- | Parameters of the current connection.
-- We store only the parameters that cannot change after startup.
-- For more information about additional parameters see documentation.
data ConnectionParameters = ConnectionParameters
    { paramServerVersion    :: ServerVersion
    , paramServerEncoding   :: B.ByteString -- ^ character set name
    , paramIntegerDatetimes :: Bool         -- ^ True if integer datetimes used
    } deriving (Show)

-- | Server version contains major, minor, revision numbers.
data ServerVersion = ServerVersion Word8 Word8 Word8

instance Show ServerVersion where
    show (ServerVersion major minor revision) =
        show major ++ "." ++ show minor ++ "." ++ show revision

data TransactionStatus
    = TransactionIdle
    | TransactionInProgress
    | TransactionFailed
    deriving (Show)

data Format = Text | Binary
    deriving (Show)

-- All the commands have the same names as presented in the official
-- postgres documentation except explicit exclusions
data AuthResponse
    = AuthenticationOk
    | AuthenticationCleartextPassword
    | AuthenticationMD5Password MD5Salt
    | AuthenticationGSS
    | AuthenticationSSPI
    -- TODO improve
    | AuthenticationGSSContinue B.ByteString
    deriving (Show)

data ClientMessage
    = Bind PortalName StatementName
        Format                  -- parameter format code, one format for all
        (V.Vector B.ByteString) -- the values of parameters
        Format                  -- to apply code to all result columns
    -- Postgres use one command `close` for closing both statements and
    -- portals, but we distinguish them
    | CloseStatement StatementName
    | ClosePortal PortalName
    -- Postgres use one command `describe` for describing both statements
    -- and portals, but we distinguish them
    | DescribeStatement StatementName
    | DescribePortal PortalName
    | Execute PortalName
    | Flush
    | Parse StatementName StatementSQL (V.Vector Oid)
    -- TODO maybe distinguish plain passwords and encrypted
    | PasswordMessage PasswordText
    -- PostgreSQL names it `Query`
    | SimpleQuery StatementSQL
    | Sync
    | Terminate
    deriving (Show)

data StartMessage
    = StartupMessage Username DatabaseName
    | SSLRequest
    deriving (Show)

data ServerMessage
    = BackendKeyData ServerProccessId ServerSecretKey
    | BindComplete
    | CloseComplete
    | CommandComplete CommandTag
    | DataRow (V.Vector B.ByteString) -- the values of a result
    | EmptyQueryResponse
    | ErrorResponse ErrorDesc
    | NoData
    -- We dont store content of notice at all
    | NoticeResponse
    | NotificationResponse
        ServerProccessId
        ChannelName
        B.ByteString -- payload - does not have structure
    | ParameterDescription (V.Vector Oid)
    -- parameter name and its value
    | ParameterStatus B.ByteString B.ByteString
    | ParseComplete
    | PortalSuspended
    | ReadForQuery TransactionStatus
    | RowDescription (V.Vector FieldDescription)
    deriving (Show)

data FieldDescription = FieldDescription {
    -- the name
      fieldName :: B.ByteString
    -- the object ID of the table
    , fieldTableOid :: Oid
    --  the attribute number of the column;
    , fieldColumnNumber :: Int16
    -- Oid type
    , fieldTypeOid :: Oid
    -- The data type size (see pg_type.typlen). Note that negative
    -- values denote variable-width types.
    , fieldSize :: Int16
    -- The type modifier (see pg_attribute.atttypmod).
    , fieldMode :: Int32
    -- In a RowDescription returned from the statement variant of Describe,
    -- the format code is not yet known and will always be zero.
    , fieldFormat :: Format
    } deriving (Show)

data ErrorSeverity
    = SeverityError
    | SeverityFatal
    | SeverityPanic
    | UnknownErrorSeverity
    deriving (Show, Eq)

data ErrorDesc = ErrorDesc
    { errorSeverity   :: ErrorSeverity
    , errorCode       :: B.ByteString
    , errorMessage    :: B.ByteString
    , errorDetail     :: Maybe B.ByteString
    , errorHint       :: Maybe B.ByteString
    , errorQuery      :: Maybe B.ByteString
    , errorSchema     :: Maybe B.ByteString
    , errorTable      :: Maybe B.ByteString
    , errorColumn     :: Maybe B.ByteString
    , errorDataType   :: Maybe B.ByteString
    , errorConstraint :: Maybe B.ByteString
    } deriving (Show)

-- TODO
-- * CancelRequest
-- * COPY subprotocol commands
-- * function call, is deprecated by postgres
-- * AuthenticationKerberosV5 IS deprecated by postgres
-- * AuthenticationSCMCredential IS deprecated since postgres 9.1
-- * NOTICE execute command can have number of rows to receive, but we
--   dont support this feature
-- * NOTICE bind command can have different formats for parameters and results
--   but we assume that there will be one format for all.
-- * We dont store parameters of connection that may change after startup
-- * We dont store all possible message fields in error|notice responses
-- * We dont parse content of notice response

