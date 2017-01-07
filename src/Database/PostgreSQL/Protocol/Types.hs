module Database.PostgreSQL.Protocol.Types where

import Data.Word
import qualified Data.ByteString as B
import qualified Data.Vector as V

type PortalName = B.ByteString
type StatementName = B.ByteString
type Oid = Word32
type TransactionStatus = Word8

data Format = Text | Binary
    deriving (Show)

data AuthResponse
    = AuthenticationOk
    | AuthenticationKerberosV5
    | AuthenticationCleartextPassword
    | AuthenticationMD5Password Word32
    | AuthenticationSCMCredential
    | AuthenticationGSS
    | AuthenticationSSPI
    | AuthenticationGSSContinue B.ByteString

data ClientMessage
    = Bind PortalName StatementName (V.Vector Format) (V.Vector B.ByteString)
                                    (V.Vector Format)
    | Close B.ByteString
    | Describe B.ByteString
    | Execute PortalName Word32
    | Flush
    | Parse StatementName B.ByteString (V.Vector Oid)
    | PasswordMessage B.ByteString
    | Query B.ByteString
    | Sync
    | Terminate

type Username = B.ByteString
type DatabaseName = B.ByteString

data StartMessage
    = StartupMessage Username DatabaseName
    | SSLRequest



data ServerMessage
    = BackendKeyData Word32
    | BindComplete
    | CloseComplete
    | CommandComplete B.ByteString
    | DataRow B.ByteString
    | EmptyQueryResponse
    | ErrorResponse (Maybe B.ByteString)
    | NoData
    | NoticeResponse (Maybe B.ByteString)
    | NotificationResponse Word32 B.ByteString B.ByteString
    | ParameterDescription (V.Vector Oid)
    | ParameterStatus B.ByteString B.ByteString
    | ParseComplete
    | PortalSuspended
    | ReadForQuery TransactionStatus
    | RowDescription (V.Vector FieldDescription)
    deriving (Show)

data FieldDescription = FieldDescription
    { fieldName :: B.ByteString
    , fieldTableOid :: Oid
    , fieldColumnNumber :: Word16
    , fieldTypeOid :: Oid
    , fieldSize :: Word16
    , fieldMode :: Word32
    , fieldFormat :: Format
    } deriving (Show)

-- TODO
-- | CancelRequest
-- COPY subprotocol commands
-- function call

