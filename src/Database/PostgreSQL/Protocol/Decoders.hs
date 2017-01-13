module Database.PostgreSQL.Protocol.Decoders where

import Data.Word
import Data.Int
import Data.Monoid
import Data.Foldable
import Data.Char (chr)
import Control.Applicative
import Control.Monad
import qualified Data.Vector as V
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Binary.Get

import Database.PostgreSQL.Protocol.Types

decodeAuthResponse :: Get AuthResponse
decodeAuthResponse = do
    c <- getWord8
    when ('R' /= chr (fromIntegral c)) $ fail "Invalid message"
    len <- getInt32be
    rType <- getInt32be
    case rType of
        0 -> pure AuthenticationOk
        3 -> pure AuthenticationCleartextPassword
        5 -> AuthenticationMD5Password <$> getWord32be
        7 -> pure AuthenticationGSS
        9 -> pure AuthenticationSSPI
        8 -> AuthenticationGSSContinue <$> getByteString (fromIntegral $ len -8)
        _ -> fail "Unknown authentication response"

decodeServerMessage :: Get ServerMessage
decodeServerMessage = do
    c <- getWord8
    len <- getInt32be
    case chr $ fromIntegral c of
        'K' -> BackendKeyData <$> getInt32be <*> getInt32be
        '2' -> pure BindComplete
        '3' -> pure CloseComplete
        'C' -> CommandComplete <$> getByteString (fromIntegral $ len - 4)
        'D' -> do
            columnCount <- fromIntegral <$> getInt16be
            DataRow <$> V.replicateM columnCount
                (getInt32be >>= getByteString . fromIntegral)
        'I' -> pure EmptyQueryResponse
        -- TODO
        'E' -> do
            getByteString (fromIntegral $ len - 4)
            pure $ ErrorResponse Nothing
        'n' -> pure NoData
        -- TODO
        'N' -> do
            getByteString (fromIntegral $ len - 4)
            pure $ NoticeResponse Nothing
        'A' -> NotificationResponse <$> getInt32be
                <*> decodePgString <*> decodePgString
        't' -> do
            paramCount <- fromIntegral <$> getInt16be
            ParameterDescription <$> V.replicateM paramCount getInt32be
        'S' -> ParameterStatus <$> decodePgString <*> decodePgString
        '1' -> pure ParseComplete
        's' -> pure PortalSuspended
        'Z' -> ReadForQuery <$> decodeTransactionStatus
        'T' -> do
            rowsCount <- fromIntegral <$> getInt16be
            RowDescription <$> V.replicateM rowsCount decodeFieldDescription

decodeTransactionStatus :: Get TransactionStatus
decodeTransactionStatus =  getWord8 >>= \t ->
    case chr $ fromIntegral t of
        'I' -> pure TransactionIdle
        'T' -> pure TransactionInProgress
        'E' -> pure TransactionFailed
        _   -> fail "unknown transaction status"

decodeFieldDescription :: Get FieldDescription
decodeFieldDescription = FieldDescription
    <$> decodePgString
    <*> getInt32be
    <*> getInt16be
    <*> getInt32be
    <*> getInt16be
    <*> getInt32be
    <*> decodeFormat

decodeFormat :: Get Format
decodeFormat = getInt16be >>= \f ->
    case f of
        0 -> pure Text
        1 -> pure Binary
        _ -> fail "Unknown field format"

decodePgString :: Get B.ByteString
decodePgString = BL.toStrict <$> getLazyByteStringNul

