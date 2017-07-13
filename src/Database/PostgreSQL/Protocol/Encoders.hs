module Database.PostgreSQL.Protocol.Encoders
    ( encodeStartMessage
    , encodeClientMessage
    ) where

import           Data.Word (Word32)
import           Data.Monoid ((<>))
import           Data.Char (ord)
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Encode

-- | Protocol Version 3.0, major version in the first word16.
currentVersion :: Word32
currentVersion = 3 * 256 * 256

encodeStartMessage :: StartMessage -> Encode
encodeStartMessage (StartupMessage (Username uname) (DatabaseName dbname))
    = putWord32BE (len + 4) <> payload
  where
    len     = fromIntegral $ getEncodeLen payload
    payload = putWord32BE currentVersion <>
              putByteStringNull "user" <> 
              putByteStringNull uname <>
              putByteStringNull "database" <> 
              putByteStringNull dbname <> 
              putWord8 0
encodeStartMessage SSLRequest
    -- Value hardcoded by PostgreSQL docs.
    = putWord32BE 8 <> putWord32BE 80877103 

encodeClientMessage :: ClientMessage -> Encode
encodeClientMessage (Bind (PortalName portalName) (StatementName stmtName)
                     paramFormat values resultFormat)
    = prependHeader 'B' $
        putByteStringNull portalName <>
        putByteStringNull stmtName <>
        -- `1` means that the specified format code is applied to all parameters
        putWord16BE 1 <>
        encodeFormat paramFormat <>
        putWord16BE (fromIntegral $ length values) <>
        foldMap encodeValue values <>
        -- `1` means that the specified format code is applied to all
        -- result columns (if any)
        putWord16BE 1 <>
        encodeFormat resultFormat
encodeClientMessage (CloseStatement (StatementName stmtName))
    = prependHeader 'C' $ putChar8 'S' <> putByteStringNull stmtName
encodeClientMessage (ClosePortal (PortalName portalName))
    = prependHeader 'C' $ putChar8 'P' <> putByteStringNull portalName
encodeClientMessage (DescribeStatement (StatementName stmtName))
    = prependHeader 'D' $ putChar8 'S' <> putByteStringNull stmtName
encodeClientMessage (DescribePortal (PortalName portalName))
    = prependHeader 'D' $ putChar8 'P' <> putByteStringNull portalName
encodeClientMessage (Execute (PortalName portalName) (RowsToReceive rows))
    = prependHeader 'E' $
        putByteStringNull portalName <>
        putWord32BE rows
encodeClientMessage Flush
    = prependHeader 'H' mempty
encodeClientMessage (Parse (StatementName stmtName) (StatementSQL stmt) oids)
    = prependHeader 'P' $
        putByteStringNull stmtName <>
        putByteStringNull stmt <>
        putWord16BE (fromIntegral $ length oids) <>
        foldMap (putWord32BE . unOid) oids
encodeClientMessage (PasswordMessage passtext)
    = prependHeader 'p' $ putByteStringNull $ getPassword passtext
      where
        getPassword (PasswordPlain p) = p
        getPassword (PasswordMD5 p) = p
encodeClientMessage (SimpleQuery (StatementSQL stmt))
    = prependHeader 'Q' $ putByteStringNull stmt
encodeClientMessage Sync
    = prependHeader 'S' mempty
encodeClientMessage Terminate
    = prependHeader 'X' mempty

-- | Encodes single data values. Length `-1` indicates a NULL parameter value.
-- No value bytes follow in the NULL case.
{-# INLINE encodeValue #-}
encodeValue :: Maybe Encode -> Encode
encodeValue Nothing  = putWord32BE (-1)
encodeValue (Just v) = putWord32BE (fromIntegral $ getEncodeLen v) <> v

{-# INLINE encodeFormat #-}
encodeFormat :: Format -> Encode
encodeFormat Text   = putWord16BE 0
encodeFormat Binary = putWord16BE 1

{-# INLINE prependHeader #-}
prependHeader :: Char -> Encode -> Encode
prependHeader c payload =
   -- Length includes itself but not the first message-type byte
    let len = 4 + fromIntegral (getEncodeLen payload)
    in putChar8 c <> putWord32BE len <> payload

{-# INLINE putChar8 #-}
putChar8 :: Char -> Encode
putChar8 = putWord8 . fromIntegral . ord
