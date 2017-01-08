module Database.PostgreSQL.Protocol.Types where

import Data.Word
import qualified Data.ByteString as B
import qualified Data.Vector as V

type PortalName = B.ByteString
type StatementName = B.ByteString
type Oid = Word32
-- maybe distinguish sql for extended query and simple query
type StatementSQL = B.ByteString
type PasswordText = B.ByteString
type ServerProccessId = Word32
type ServerSecretKey = Word32
-- String that identifies which SQL command was completed.
-- should be more complex in future
type CommandTag = B.ByteString

data TransactionStatus
    = TransactionIdle
    | TransactionInProgress
    | TransactionFailed

data Format = Text | Binary
    deriving (Show)

-- All the commands have the same names as presented in the official
-- postgres documentation except explicit exclusions
-- TODO improve
data AuthResponse
    = AuthenticationOk
    | AuthenticationCleartextPassword
    | AuthenticationMD5Password Word32
    | AuthenticationGSS
    | AuthenticationSSPI
    | AuthenticationGSSContinue B.ByteString

data ClientMessage
    = Bind PortalName StatementName
        (V.Vector Format)       -- parameter format codes
        (V.Vector B.ByteString) -- the values of parameters
        (V.Vector Format)       -- format codes of result,
                                -- maybe chaneg to one number
                                -- to apply code to all result columns
    -- Postgres use one command `close` for closing both statements and
    -- portals, but we distinguish them
    | CloseStatement StatementName
    | ClosePortal PortalName
    -- Postgres use one commande `describe` for describing both statements
    -- and portals, but we distinguish them
    | DescribeStatement StatementName
    | DescribePortal PortalName
    | Execute PortalName
    | Flush
    | Parse StatementName StatementSQL (V.Vector Oid)
    | PasswordMessage PasswordText
    | Query StatementSQL
    | Sync
    | Terminate

type Username = B.ByteString
type DatabaseName = B.ByteString

data StartMessage
    = StartupMessage Username DatabaseName
    | SSLRequest



data ServerMessage
    = BackendKeyData ServerProccessId ServerSecretKey
    | BindComplete
    | CloseComplete
    | CommandComplete CommandTag
    | DataRow (V.Vector B.ByteString) -- the values of a result
    | EmptyQueryResponse
    -- TODO change to list of error fields
    | ErrorResponse (Maybe B.ByteString)
    | NoData
    -- TODO change to list of fields
    | NoticeResponse (Maybe B.ByteString)
    | NotificationResponse
        ServerProccessId
        B.ByteString -- the name of the channel
        B.ByteString -- payload - does not have structure
    | ParameterDescription (V.Vector Oid)
    -- parameter name and its value
    | ParameterStatus B.ByteString B.ByteString
    | ParseComplete
    | PortalSuspended
    | ReadForQuery TransactionStatus
    | RowDescription (V.Vector FieldDescription)
    deriving (Show)

data FieldDescription = FieldDescription
    { fieldName :: B.ByteString
    -- the object ID of the table
    , fieldTableOid :: Oid
    --  the attribute number of the column;
    , fieldColumnNumber :: Word16
    , fieldTypeOid :: Oid
    -- The data type size (see pg_type.typlen). Note that negative
    -- values denote variable-width types.
    , fieldSize :: Word16
    -- The type modifier (see pg_attribute.atttypmod).
    , fieldMode :: Word32
    -- In a RowDescription returned from the statement variant of Describe,
    -- the format code is not yet known and will always be zero.
    , fieldFormat :: Format
    } deriving (Show)

-- TODO
-- * CancelRequest
-- * COPY subprotocol commands
-- * function call, is deprecated by postgres
-- * AuthenticationKerberosV5 IS deprecated by postgres
-- * AuthenticationSCMCredential IS deprecated since postgres 9.1
-- * NOTICE execute command can have number of rows to receive, but we
--   dont support this feature

