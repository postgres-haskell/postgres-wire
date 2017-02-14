{-# language RecordWildCards #-}

module Database.PostgreSQL.Protocol.Decoders
    ( decodeAuthResponse
    , decodeHeader
    , decodeServerMessage
    -- * Helpers
    , parseServerVersion
    , parseIntegerDatetimes
    , loopExtractDataRows
    , loopParseServerMessages
    ) where

import           Control.Applicative
import           Control.Monad
import           Data.Monoid ((<>))
import           Data.Maybe (fromMaybe)
import           Data.Char (chr)
import           Data.Word
import           Foreign
import           Text.Read (readMaybe)
import qualified Data.Vector as V
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Internal as BL
import           Data.ByteString.Char8 as BS(readInteger, readInt, unpack, pack)
import qualified Data.HashMap.Strict as HM

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Utils

-- Optimized loop for extracting chunks of DataRows.
-- Ignores all messages from database that do not relate to data.
-- Does not throw exceptions.
loopExtractDataRows
    -- Action that returs more data with `ByteString` prepended.
    :: (B.ByteString -> IO B.ByteString)
    -- Will be called on every DataMessage.
    -> (DataMessage -> IO ())
    -> IO ()
loopExtractDataRows readMoreAction callback = go "" ""
  where
    go :: B.ByteString -> BL.ByteString -> IO ()
    go bs acc
        -- 5 - header size, defined by PostgreSQL
        | B.length bs < 5 = readMoreAndGo bs acc
        | otherwise = do
            ScanRowResult ch rest r <- scanDataRows bs
            -- We should force accumulator
            -- Note: `BL.chunk` should not prepend empty bytestring as chunk.
            let !newAcc = BL.chunk ch acc

            case r of
                -- Following happened:
                --   not enough bytes to read header
                --   or header is for `DataRow`, not enough bytes to read body
                1 -> readMoreAndGo rest newAcc
                -- Header was read, it is not for `DataRow`. We can safely
                -- call `parseHeader`, because scanDataRows already checked
                -- that there are enough bytes to read header.
                2 -> do
                    Header mt len <- parseHeader rest
                    dispatchHeader mt len (B.drop 5 rest) newAcc

    {-# INLINE dispatchHeader #-}
    dispatchHeader :: Word8 -> Int -> B.ByteString -> BL.ByteString -> IO ()
    dispatchHeader mt len bs acc = case mt of
        -- 'C' - CommandComplete.
        -- Command is completed, return the result.
        67 -> do
            callback $
                DataMessage . DataRows $
                    BL.foldlChunks (flip BL.chunk) BL.empty acc

            newBs <- skipBytes bs len
            go newBs BL.empty

        -- 'I' - EmptyQueryResponse.
        -- PostgreSQL sends this if query string was empty and datarows
        -- should be empty, but anyway we return data collected in `acc`.
        73 -> do
            callback $
                DataMessage . DataRows $
                    BL.foldlChunks (flip BL.chunk) BL.empty acc

            go bs BL.empty

        -- 'E' - ErrorResponse.
        -- On ErrorResponse we should discard all the collected datarows.
        69 -> do
            (b, newBs) <- readAtLeast bs len
            desc <- eitherToDecode $ parseErrorDesc b
            callback (DataError desc)

            go newBs BL.empty

        -- 'Z' - ReadyForQuery.
        -- To know when command processing is finished
        90 -> do
            callback DataReady

            newBs <- skipBytes bs len
            go newBs acc

       -- Skip any other message.
        _   -> do
            newBs <- skipBytes bs len
            go newBs acc

    {-# INLINE readMoreAndGo #-}
    readMoreAndGo :: B.ByteString -> BL.ByteString -> IO ()
    readMoreAndGo bs acc = do
        newBs <- readMoreAction bs
        go newBs acc

    -- | Returns a bytestring that contain exactly @len@ bytes and the rest.
    {-# INLINE readAtLeast #-}
    readAtLeast :: B.ByteString -> Int -> IO (B.ByteString, B.ByteString)
    readAtLeast bs len
        | B.length bs >= len = pure $ B.splitAt len bs
        | otherwise = do
            newBs <- readMoreAction bs
            readAtLeast newBs len

    -- | Skips exactly @toSkip@ bytes.
    {-# INLINE skipBytes #-}
    skipBytes :: B.ByteString -> Int -> IO B.ByteString
    skipBytes bs toSkip
        | toSkip <= 0          = pure bs
        | B.length bs < toSkip = do
            newBs <- readMoreAction B.empty
            skipBytes newBs (toSkip - B.length bs)
        | otherwise            = pure $ B.drop toSkip bs

    {-# INLINE parseHeader #-}
    parseHeader :: B.ByteString -> IO Header
    parseHeader bs =
        B.unsafeUseAsCStringLen bs $ \(ptr, len) -> do
            b <- peek (castPtr ptr)
            w <- byteSwap32 <$> peekByteOff (castPtr ptr) 1
            pure $ Header b $ fromIntegral (w - 4)

-- | Loop that parses and dispatches all server messages except `DataRow`.
loopParseServerMessages
    -- Action that returs more data with `ByteString` prepended.
    :: (B.ByteString -> IO B.ByteString)
    -- Will be called on every ServerMessage.
    -> (ServerMessage -> IO ())
    -> IO ()
loopParseServerMessages readMoreAction callback = go Nothing ""
  where
    -- Parse header
    go Nothing bs
        | B.length bs < 5 = readMoreAndGo Nothing bs
        | otherwise = let (rest, h) = runDecode decodeHeader bs
                      in go (Just h) rest
    -- Parse body
    go (Just h@(Header _ len)) bs
        | B.length bs < len = readMoreAndGo (Just h) bs
        | otherwise = let (rest, v) = runDecode (decodeServerMessage h) bs
                      in callback v >> go Nothing rest

    {-# INLINE readMoreAndGo #-}
    readMoreAndGo :: Maybe Header -> B.ByteString -> IO ()
    readMoreAndGo h = (go h =<<) . readMoreAction


decodeAuthResponse :: Decode AuthResponse
decodeAuthResponse = do
    c <- getWord8
    len <- getInt32BE
    case chr $ fromIntegral c of
        'E' -> AuthErrorResponse <$>
            (getByteString (fromIntegral $ len - 4) >>=
                eitherToDecode .parseErrorDesc)
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

decodeHeader :: Decode Header
decodeHeader = Header <$> getWord8 <*>
                (fromIntegral . subtract 4 <$> getInt32BE)

decodeServerMessage :: Header -> Decode ServerMessage
decodeServerMessage (Header c len) = case chr $ fromIntegral c of
    'K' -> BackendKeyData <$> (ServerProcessId <$> getInt32BE)
                          <*> (ServerSecretKey <$> getInt32BE)
    '2' -> pure BindComplete
    '3' -> pure CloseComplete
    'C' -> CommandComplete <$> (getByteString len
                                >>= eitherToDecode . parseCommandResult)
    -- Dont parse data rows here.
    'D' -> do
        bs <- getByteString len
        pure $ DataRow ("abcde" <> bs)
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
decodeValue = getInt32BE >>= \n ->
    if n == -1
    then pure Nothing
    else Just <$> getByteString (fromIntegral n)

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

-- Parser that just work with B.ByteString, not Decode type

-- Helper to parse, not used by decoder itself
parseServerVersion :: B.ByteString -> Maybe ServerVersion
parseServerVersion bs =
    let (numbersStr, desc) = B.span isDigitDot bs
        numbers = readMaybe . BS.unpack <$> B.split 46 numbersStr
    in case numbers ++ repeat (Just 0) of
        (Just major : Just minor : Just rev : _) ->
            Just $ ServerVersion major minor rev desc
        _ -> Nothing
  where
    isDigitDot c | c == 46           = True -- dot
                 | c >= 48 && c < 58 = True -- digits
                 | otherwise         = False

-- Helper to parse, not used by decoder itself
parseIntegerDatetimes :: B.ByteString -> Bool
parseIntegerDatetimes  bs | bs == "on" || bs == "yes" || bs == "1" = True
                          | otherwise                              = False

parseCommandResult :: B.ByteString -> Either B.ByteString CommandResult
parseCommandResult s =
    let (command, rest) = B.break (== space) s
    in case command of
        -- format: `INSERT oid rows`
        "INSERT" ->
            maybe (Left "Invalid format in INSERT command result") Right $ do
                (oid, r) <- readInteger $ B.dropWhile (== space) rest
                (rows, _) <- readInteger $ B.dropWhile (== space) r
                Just $ InsertCompleted (Oid $ fromInteger oid)
                                       (RowsCount $ fromInteger rows)
        "DELETE" -> DeleteCompleted <$> readRows rest
        "UPDATE" -> UpdateCompleted <$> readRows rest
        "SELECT" -> SelectCompleted <$> readRows rest
        "MOVE"   -> MoveCompleted   <$> readRows rest
        "FETCH"  -> FetchCompleted  <$> readRows rest
        "COPY"   -> CopyCompleted   <$> readRows rest
        _        -> Right CommandOk
  where
    space = 32
    readRows = maybe (Left "Invalid rows format in command result")
                       (pure . RowsCount . fromInteger . fst)
                       . readInteger . B.dropWhile (== space)

parseErrorNoticeFields :: B.ByteString -> HM.HashMap Char B.ByteString
parseErrorNoticeFields = HM.fromList
    . fmap (\s -> (chr . fromIntegral $ B.head s, B.tail s))
    . filter (not . B.null) . B.split 0

parseErrorSeverity :: B.ByteString -> ErrorSeverity
parseErrorSeverity bs = case bs of
    "ERROR" -> SeverityError
    "FATAL" -> SeverityFatal
    "PANIC" -> SeverityPanic
    _       -> UnknownErrorSeverity

parseNoticeSeverity :: B.ByteString -> NoticeSeverity
parseNoticeSeverity bs = case bs of
    "WARNING" -> SeverityWarning
    "NOTICE"  -> SeverityNotice
    "DEBUG"   -> SeverityDebug
    "INFO"    -> SeverityInfo
    "LOG"     -> SeverityLog
    _         -> UnknownNoticeSeverity

parseErrorDesc :: B.ByteString -> Either B.ByteString ErrorDesc
parseErrorDesc s = do
    let hm = parseErrorNoticeFields s
    errorSeverityOld <- lookupKey 'S' hm
    errorCode        <- lookupKey 'C' hm
    errorMessage     <- lookupKey 'M' hm
    let
        -- This is identical to the S field except that the contents are
        -- never localized. This is present only in messages generated by
        -- PostgreSQL versions 9.6 and later.
        errorSeverityNew      = HM.lookup 'V' hm
        errorSeverity         = parseErrorSeverity $
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
    Right ErrorDesc{..}
  where
    lookupKey c = maybe (Left $ "Neccessary key " <> BS.pack (show c) <>
                         "is not presented in ErrorResponse message")
                         Right . HM.lookup c

parseNoticeDesc :: B.ByteString -> Either B.ByteString NoticeDesc
parseNoticeDesc s = do
    let hm = parseErrorNoticeFields s
    noticeSeverityOld <- lookupKey 'S' hm
    noticeCode        <- lookupKey 'C' hm
    noticeMessage     <- lookupKey 'M' hm
    let
        -- This is identical to the S field except that the contents are
        -- never localized. This is present only in messages generated by
        -- PostgreSQL versions 9.6 and later.
        noticeSeverityNew      = HM.lookup 'V' hm
        noticeSeverity         = parseNoticeSeverity $
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
    Right NoticeDesc{..}
  where
    lookupKey c = maybe (Left $ "Neccessary key " <> BS.pack (show c) <>
                         "is not presented in NoticeResponse message")
                         Right . HM.lookup c

eitherToDecode :: Monad m => Either B.ByteString a -> m a
eitherToDecode = either (fail . BS.unpack) pure

