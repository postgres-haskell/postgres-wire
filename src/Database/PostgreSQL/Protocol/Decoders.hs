module Database.PostgreSQL.Protocol.Decoders
    ( 
    -- * High-lever decoder
      decodeNextServerMessage
    -- * Decoders
    , decodeAuthResponse
    , decodeHeader
    , decodeServerMessage
    ) where

import Data.Char (chr)

import Data.ByteString.Char8 as BS(unpack)
import qualified Data.ByteString as B
import qualified Data.Vector as V

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Parsers

-- | Parses and dispatches all server messages except `DataRow`.
decodeNextServerMessage
    -- Initial buffer to parse from
    :: B.ByteString
    -- Action that returs more data with `ByteString` prepended.
    -> (B.ByteString -> IO B.ByteString)
    -> IO (B.ByteString, ServerMessage)
decodeNextServerMessage bs readMoreAction = go Nothing bs
  where
    -- Parse header
    go Nothing bs
        | B.length bs < headerSize = readMoreAndGo Nothing bs
        | otherwise = let (rest, h) = runDecode decodeHeader bs
                      in go (Just h) rest
    -- Parse body
    go (Just h@(Header _ len)) bs
        | B.length bs < len = readMoreAndGo (Just h) bs
        | otherwise = pure $ runDecode (decodeServerMessage h) bs

    {-# INLINE readMoreAndGo #-}
    readMoreAndGo h = (go h =<<) . readMoreAction

--------------------------------
-- Protocol decoders

decodeAuthResponse :: Decode AuthResponse
decodeAuthResponse = do
    Header c len <- decodeHeader
    case chr $ fromIntegral c of
        'E' -> AuthErrorResponse <$>
            (getByteString len >>=
                eitherToDecode .parseErrorDesc)
        'R' -> do
            rType <- getWord32BE
            case rType of
                0 -> pure AuthenticationOk
                3 -> pure AuthenticationCleartextPassword
                5 -> AuthenticationMD5Password . MD5Salt <$> getByteString 4
                7 -> pure AuthenticationGSS
                9 -> pure AuthenticationSSPI
                8 -> AuthenticationGSSContinue <$> getByteString (len - 4)
                _ -> fail "Unknown authentication response"
        _ -> fail "Invalid auth response"

{-# INLINE decodeHeader #-}
decodeHeader :: Decode Header
decodeHeader = Header <$> getWord8 <*>
                (fromIntegral . subtract 4 <$> getWord32BE)

decodeServerMessage :: Header -> Decode ServerMessage
decodeServerMessage (Header c len) = case chr $ fromIntegral c of
    'K' -> BackendKeyData <$> (ServerProcessId <$> getWord32BE)
                          <*> (ServerSecretKey <$> getWord32BE)
    '2' -> pure BindComplete
    '3' -> pure CloseComplete
    'C' -> CommandComplete <$> (getByteString len
                                >>= eitherToDecode . parseCommandResult)
    -- Dont parse data rows here.
    'D' -> do
      bs <- getByteString len
      pure $! DataRow bs
    'I' -> pure EmptyQueryResponse
    'E' -> ErrorResponse <$>
        (getByteString len >>=
            eitherToDecode . parseErrorDesc)
    'n' -> pure NoData
    'N' -> NoticeResponse <$>
        (getByteString len >>=
            eitherToDecode . parseNoticeDesc)
    'A' -> NotificationResponse <$> decodeNotification
    't' -> do
        paramCount <- fromIntegral <$> getWord16BE
        ParameterDescription <$> V.replicateM paramCount
                                 (Oid <$> getWord32BE)
    'S' -> ParameterStatus <$> getByteStringNull <*> getByteStringNull
    '1' -> pure ParseComplete
    's' -> pure PortalSuspended
    'Z' -> ReadyForQuery <$> decodeTransactionStatus
    'T' -> do
        rowsCount <- fromIntegral <$> getWord16BE
        RowDescription <$> V.replicateM rowsCount decodeFieldDescription

{-# INLINE decodeTransactionStatus #-}
decodeTransactionStatus :: Decode TransactionStatus
decodeTransactionStatus =  getWord8 >>= \t ->
    case chr $ fromIntegral t of
        'I' -> pure TransactionIdle
        'T' -> pure TransactionInBlock
        'E' -> pure TransactionFailed
        _   -> fail "unknown transaction status"

decodeFieldDescription :: Decode FieldDescription
decodeFieldDescription = FieldDescription
    <$> getByteStringNull
    <*> (Oid <$> getWord32BE)
    <*> getWord16BE
    <*> (Oid <$> getWord32BE)
    <*> getInt16BE
    <*> getInt32BE
    <*> decodeFormat

{-# INLINE decodeNotification #-}
decodeNotification :: Decode Notification
decodeNotification = Notification
    <$> (ServerProcessId <$> getWord32BE)
    <*> (ChannelName <$> getByteStringNull)
    <*> getByteStringNull

{-# INLINE decodeFormat #-}
decodeFormat :: Decode Format
decodeFormat = getWord16BE >>= \f ->
    case f of
        0 -> pure Text
        1 -> pure Binary
        _ -> fail "Unknown field format"

-- | Helper to lift Either in Decode
{-# INLINE eitherToDecode #-}
eitherToDecode :: Either B.ByteString a -> Decode a
eitherToDecode = either (fail . BS.unpack) pure

