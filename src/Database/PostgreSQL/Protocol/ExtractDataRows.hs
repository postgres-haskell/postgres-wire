module Database.PostgreSQL.Protocol.ExtractDataRows 
    ( loopExtractDataRows
    ) where

import Data.Word    (Word8, byteSwap32)
import Foreign      (peek, peekByteOff, castPtr)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Internal as BL

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Decoders
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
            -- TODO handle errors
            desc <- eitherToProtocolEx $  parseErrorDesc b
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
