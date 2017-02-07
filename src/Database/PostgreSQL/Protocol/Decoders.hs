{-# language RecordWildCards #-}
module Database.PostgreSQL.Protocol.Decoders where

import Data.Word
import Data.Int
import Data.Monoid
import Data.Maybe (fromMaybe)
import Data.Foldable
import Data.Char (chr)
import Control.Applicative
import Control.Monad
import qualified Data.Vector as V
import qualified Data.ByteString as B
import           Data.ByteString.Char8 (readInteger, readInt)
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Decode

decodeAuthResponse :: Decode AuthResponse
decodeAuthResponse = do
    c <- getWord8
    len <- getInt32BE
    case chr $ fromIntegral c of
        'E' -> AuthErrorResponse <$>
            (getByteString (fromIntegral $ len - 4) >>= decodeErrorDesc)
        'R' -> do
            rType <- getInt32BE
            case rType of
                0 -> pure AuthenticationOk
                3 -> pure AuthenticationCleartextPassword
                5 -> AuthenticationMD5Password . MD5Salt <$> getByteString 4
                7 -> pure AuthenticationGSS
                9 -> pure AuthenticationSSPI
                8 -> AuthenticationGSSContinue <$>
                        getByteString (fromIntegral $ len -8)
                _ -> fail "Unknown authentication response"
        _ -> fail "Invalid auth response"

decodeServerMessage :: Decode ServerMessage
decodeServerMessage = do
    c <- getWord8
    len <- getInt32BE
    case chr $ fromIntegral c of
        'K' -> BackendKeyData <$> (ServerProcessId <$> getInt32BE)
                              <*> (ServerSecretKey <$> getInt32BE)
        '2' -> pure BindComplete
        '3' -> pure CloseComplete
        'C' -> CommandComplete <$> (getByteString (fromIntegral $ len - 4)
                                    >>= decodeCommandResult)
        'D' -> do
            columnCount <- fromIntegral <$> getInt16BE
            DataRow <$> V.replicateM columnCount decodeValue
        'I' -> pure EmptyQueryResponse
        'E' -> ErrorResponse <$>
            (getByteString (fromIntegral $ len - 4) >>= decodeErrorDesc)
        'n' -> pure NoData
        'N' -> NoticeResponse <$>
            (getByteString (fromIntegral $ len - 4) >>= decodeNoticeDesc)
        'A' -> NotificationResponse <$> decodeNotification
        't' -> do
            paramCount <- fromIntegral <$> getInt16BE
            ParameterDescription <$> V.replicateM paramCount
                                     (Oid <$> getInt32BE)
        'S' -> ParameterStatus <$> getByteStringNull <*> getByteStringNull
        '1' -> pure ParseComplete
        's' -> pure PortalSuspended
        'Z' -> ReadForQuery <$> decodeTransactionStatus
        'T' -> do
            rowsCount <- fromIntegral <$> getInt16BE
            RowDescription <$> V.replicateM rowsCount decodeFieldDescription

-- | Decodes a single data value. Length `-1` indicates a NULL column value.
-- No value bytes follow in the NULL case.
decodeValue :: Decode (Maybe B.ByteString)
decodeValue = fromIntegral <$> getInt32BE >>= \n ->
    if n == -1
    then pure Nothing
    else Just <$> getByteString n

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
    <*> (Oid <$> getInt32BE)
    <*> getInt16BE
    <*> (Oid <$> getInt32BE)
    <*> getInt16BE
    <*> getInt32BE
    <*> decodeFormat

decodeNotification :: Decode Notification
decodeNotification = Notification
    <$> (ServerProcessId <$> getInt32BE)
    <*> (ChannelName <$> getByteStringNull)
    <*> getByteStringNull

decodeFormat :: Decode Format
decodeFormat = getInt16BE >>= \f ->
    case f of
        0 -> pure Text
        1 -> pure Binary
        _ -> fail "Unknown field format"

decodeCommandResult :: B.ByteString -> Decode CommandResult
decodeCommandResult s =
    let (command, rest) = B.break (== space) s
    in case command of
        -- format: `INSERT oid rows`
        "INSERT" ->
            maybe (fail "Invalid format in INSERT command result") pure $ do
                (oid, r) <- readInteger $ B.dropWhile (== space) rest
                (rows, _) <- readInteger $ B.dropWhile (== space) r
                pure $ InsertCompleted (Oid $ fromInteger oid)
                                       (RowsCount $ fromInteger rows)
        "DELETE" -> DeleteCompleted <$> readRows rest
        "UPDATE" -> UpdateCompleted <$> readRows rest
        "SELECT" -> SelectCompleted <$> readRows rest
        "MOVE"   -> MoveCompleted <$> readRows rest
        "FETCH"  -> FetchCompleted <$> readRows rest
        "COPY"   -> CopyCompleted <$> readRows rest
        _        -> pure CommandOk
  where
    space = 32
    readRows = maybe (fail "Invalid rows format in command result")
                       (pure . RowsCount . fromInteger . fst)
                       . readInteger . B.dropWhile (== space)

decodeErrorNoticeFields :: B.ByteString -> HM.HashMap Char B.ByteString
decodeErrorNoticeFields = HM.fromList
    . fmap (\s -> (chr . fromIntegral $ B.head s, B.tail s))
    . filter (not . B.null) . B.split 0

decodeErrorSeverity :: B.ByteString -> ErrorSeverity
decodeErrorSeverity "ERROR" = SeverityError
decodeErrorSeverity "FATAL" = SeverityFatal
decodeErrorSeverity "PANIC" = SeverityPanic
decodeErrorSeverity _       = UnknownErrorSeverity

decodeNoticeSeverity :: B.ByteString -> NoticeSeverity
decodeNoticeSeverity "WARNING" = SeverityWarning
decodeNoticeSeverity "NOTICE"  = SeverityNotice
decodeNoticeSeverity "DEBUG"   = SeverityDebug
decodeNoticeSeverity "INFO"    = SeverityInfo
decodeNoticeSeverity "LOG"     = SeverityLog
decodeNoticeSeverity _         = UnknownNoticeSeverity

decodeErrorDesc :: B.ByteString -> Decode ErrorDesc
decodeErrorDesc s = do
    let hm = decodeErrorNoticeFields s
    errorSeverityOld <- lookupKey 'S' hm
    errorCode        <- lookupKey 'C' hm
    errorMessage     <- lookupKey 'M' hm
    let
        -- This is identical to the S field except that the contents are
        -- never localized. This is present only in messages generated by
        -- PostgreSQL versions 9.6 and later.
        errorSeverityNew      = HM.lookup 'V' hm
        errorSeverity         = decodeErrorSeverity $
                                fromMaybe errorSeverityOld errorSeverityNew
        errorDetail           = HM.lookup 'D' hm
        errorHint             = HM.lookup 'H' hm
        errorPosition         = HM.lookup 'P' hm >>= fmap fst . readInt
        errorInternalPosition = HM.lookup 'p' hm >>= fmap fst . readInt
        errorInternalQuery    = HM.lookup 'q' hm
        errorContext          = HM.lookup 'W' hm
        errorSchema           = HM.lookup 's' hm
        errorTable            = HM.lookup 't' hm
        errorColumn           = HM.lookup 'c' hm
        errorDataType         = HM.lookup 'd' hm
        errorConstraint       = HM.lookup 'n' hm
        errorSourceFilename   = HM.lookup 'F' hm
        errorSourceLine       = HM.lookup 'L' hm >>= fmap fst . readInt
        errorSourceRoutine    = HM.lookup 'R' hm
    pure ErrorDesc{..}
  where
    lookupKey c = maybe (fail $ "Neccessary key " ++ show c ++
                         "is not presented in ErrorResponse message")
                         pure . HM.lookup c

decodeNoticeDesc :: B.ByteString -> Decode NoticeDesc
decodeNoticeDesc s = do
    let hm = decodeErrorNoticeFields s
    noticeSeverityOld <- lookupKey 'S' hm
    noticeCode        <- lookupKey 'C' hm
    noticeMessage     <- lookupKey 'M' hm
    let
        -- This is identical to the S field except that the contents are
        -- never localized. This is present only in messages generated by
        -- PostgreSQL versions 9.6 and later.
        noticeSeverityNew      = HM.lookup 'V' hm
        noticeSeverity         = decodeNoticeSeverity $
                                fromMaybe noticeSeverityOld noticeSeverityNew
        noticeDetail           = HM.lookup 'D' hm
        noticeHint             = HM.lookup 'H' hm
        noticePosition         = HM.lookup 'P' hm >>= fmap fst . readInt
        noticeInternalPosition = HM.lookup 'p' hm >>= fmap fst . readInt
        noticeInternalQuery    = HM.lookup 'q' hm
        noticeContext          = HM.lookup 'W' hm
        noticeSchema           = HM.lookup 's' hm
        noticeTable            = HM.lookup 't' hm
        noticeColumn           = HM.lookup 'c' hm
        noticeDataType         = HM.lookup 'd' hm
        noticeConstraint       = HM.lookup 'n' hm
        noticeSourceFilename   = HM.lookup 'F' hm
        noticeSourceLine       = HM.lookup 'L' hm >>= fmap fst . readInt
        noticeSourceRoutine    = HM.lookup 'R' hm
    pure NoticeDesc{..}
  where
    lookupKey c = maybe (fail $ "Neccessary key " ++ show c ++
                         "is not presented in NoticeResponse message")
                         pure . HM.lookup c

