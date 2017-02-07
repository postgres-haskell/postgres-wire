module Database.PostgreSQL.Protocol.Encoders where

import Data.Word
import Data.Int
import Data.Monoid
import Data.Foldable
import qualified Data.Vector as V
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Encode

-- | Protocol Version 3.0, major version in the first word16.
currentVersion :: Int32
currentVersion = 3 * 256 * 256

encodeStartMessage :: StartMessage -> Encode
encodeStartMessage (StartupMessage (Username uname) (DatabaseName dbname))
    = putInt32BE (len + 4) <> payload
  where
    len     = fromIntegral $ getEncodeLen payload
    payload = putInt32BE currentVersion <>
              putPgString "user" <> putPgString uname <>
              putPgString "database" <> putPgString dbname <> putWord8 0
encodeStartMessage SSLRequest
    = putInt32BE 8 <> putInt32BE 80877103 -- value hardcoded by PostgreSQL docs.

encodeClientMessage :: ClientMessage -> Encode
encodeClientMessage (Bind (PortalName portalName) (StatementName stmtName)
                     paramFormat values resultFormat)
    = prependHeader 'B' $
        putPgString portalName <>
        putPgString stmtName <>
        -- `1` means that the specified format code is applied to all parameters
        putInt16BE 1 <>
        encodeFormat paramFormat <>
        putInt16BE (fromIntegral $ V.length values) <>
        foldMap encodeValue values <>
        -- `1` means that the specified format code is applied to all
        -- result columns (if any)
        putInt16BE 1 <>
        encodeFormat resultFormat
encodeClientMessage (CloseStatement (StatementName stmtName))
    = prependHeader 'C' $ putChar8 'S' <> putPgString stmtName
encodeClientMessage (ClosePortal (PortalName portalName))
    = prependHeader 'C' $ putChar8 'P' <> putPgString portalName
encodeClientMessage (DescribeStatement (StatementName stmtName))
    = prependHeader 'D' $ putChar8 'S' <> putPgString stmtName
encodeClientMessage (DescribePortal (PortalName portalName))
    = prependHeader 'D' $ putChar8 'P' <> putPgString portalName
encodeClientMessage (Execute (PortalName portalName) (RowsToReceive rows))
    = prependHeader 'E' $
        putPgString portalName <>
        putInt32BE rows
encodeClientMessage Flush
    = prependHeader 'H' mempty
encodeClientMessage (Parse (StatementName stmtName) (StatementSQL stmt) oids)
    = prependHeader 'P' $
        putPgString stmtName <>
        putPgString stmt <>
        putInt16BE (fromIntegral $ V.length oids) <>
        foldMap (putInt32BE . unOid) oids
encodeClientMessage (PasswordMessage passtext)
    = prependHeader 'p' $ putPgString $ getPassword passtext
      where
        getPassword (PasswordPlain p) = p
        getPassword (PasswordMD5 p) = p
encodeClientMessage (SimpleQuery (StatementSQL stmt))
    = prependHeader 'Q' $ putPgString stmt
encodeClientMessage Sync
    = prependHeader 'S' mempty
encodeClientMessage Terminate
    = prependHeader 'X' mempty

-- Encodes single data values. Length `-1` indicates a NULL parameter value.
-- No value bytes follow in the NULL case.
encodeValue :: Maybe B.ByteString -> Encode
encodeValue Nothing  = putInt32BE (-1)
encodeValue (Just v) = putInt32BE (fromIntegral $ B.length v)
                            <> putByteString v

encodeFormat :: Format -> Encode
encodeFormat Text   = putInt16BE 0
encodeFormat Binary = putInt16BE 1

----------
-- Utils
---------

prependHeader :: Char -> Encode -> Encode
prependHeader c payload =
   -- Length includes itself but not the first message-type byte
    let len = 4 + fromIntegral (getEncodeLen payload)
    in putChar8 c <> putInt32BE len <> payload

