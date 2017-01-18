module Database.PostgreSQL.Protocol.Encoders where

import Data.Word
import Data.Int
import Data.Monoid
import Data.ByteString.Lazy as BL
import Data.ByteString.Builder
import Data.Foldable
import qualified Data.Vector as V
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Types

-- | Protocol Version 3.0, major version in the first word16
currentVersion :: Int32
currentVersion = 3 * 256 * 256

encodeStartMessage :: StartMessage -> Builder
-- Options except user and database are not supported
encodeStartMessage (StartupMessage (Username uname) (DatabaseName dbname)) =
    int32BE (len + 4) <> payload
  where
    len     = fromIntegral $ BL.length $ toLazyByteString payload
    payload = int32BE currentVersion <>
              pgString "user" <> pgString uname <>
              pgString "database" <> pgString dbname <> word8 0
              -- TODO
encodeStartMessage SSLRequest = undefined

encodeClientMessage :: ClientMessage -> Builder
encodeClientMessage (Bind (PortalName portalName) (StatementName stmtName)
                     paramFormat values resultFormat)
    = prependHeader 'B' $
        pgString portalName <>
        pgString stmtName <>
        -- `1` means that the specified format code is applied to all parameters
        int16BE 1 <>
        encodeFormat paramFormat <>
        int16BE (fromIntegral $ V.length values) <>
        -- TODO -1 indicates a NULL parameter value. No value bytes
        -- follow in the NULL case.
        fold ((\v -> int32BE (fromIntegral $ B.length v) <> byteString v)
              <$> values) <>
        -- `1` means that the specified format code is applied to all
        -- result columns (if any)
        int16BE 1 <>
        encodeFormat resultFormat
encodeClientMessage (CloseStatement (StatementName stmtName))
    = prependHeader 'C' $ char8 'S' <> pgString stmtName
encodeClientMessage (ClosePortal (PortalName portalName))
    = prependHeader 'C' $ char8 'P' <> pgString portalName
encodeClientMessage (DescribeStatement (StatementName stmtName))
    = prependHeader 'D' $ char8 'S' <> pgString stmtName
encodeClientMessage (DescribePortal (PortalName portalName))
    = prependHeader 'D' $ char8 'P' <> pgString portalName
encodeClientMessage (Execute (PortalName portalName))
    = prependHeader 'E' $
        pgString portalName <>
        --Maximum number of rows to return, if portal contains a query that
        --returns rows (ignored otherwise). Zero denotes "no limit".
        int32BE 0
encodeClientMessage Flush
    = prependHeader 'H' mempty
encodeClientMessage (Parse (StatementName stmtName) (StatementSQL stmt) oids)
    = prependHeader 'P' $
        pgString stmtName <>
        pgString stmt <>
        int16BE (fromIntegral $ V.length oids) <>
        fold (int32BE . unOid <$> oids)
encodeClientMessage (PasswordMessage (PasswordText passText))
    = prependHeader 'p' $ pgString passText
encodeClientMessage (SimpleQuery (StatementSQL stmt))
    = prependHeader 'Q' $ pgString stmt
encodeClientMessage Sync
    = prependHeader 'S' mempty
encodeClientMessage Terminate
    = prependHeader 'X' mempty

encodeFormat :: Format -> Builder
encodeFormat Text   = int16BE 0
encodeFormat Binary = int16BE 1

----------
-- Utils
---------

-- | C-like string
pgString :: B.ByteString -> Builder
pgString s = byteString s <> word8 0

prependHeader :: Char -> Builder -> Builder
prependHeader c builder =
    let payload = toLazyByteString builder
        -- Length includes itself but not the first message-type byte
        len = 4 + fromIntegral (BL.length payload)
    in char8 c <> int32BE len <> lazyByteString payload

