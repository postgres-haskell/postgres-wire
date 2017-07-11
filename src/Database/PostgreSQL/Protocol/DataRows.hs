module Database.PostgreSQL.Protocol.DataRows 
    ( loopExtractDataRows
    , countDataRows
    , flattenDataRows
    , decodeManyRows
    , decodeOneRow
    ) where

import Data.Monoid  ((<>))
import Data.Word    (Word8, byteSwap32)
import Foreign      (peek, peekByteOff, castPtr)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import qualified Data.List as L
import Data.Foldable
import System.IO.Unsafe

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Parsers
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
loopExtractDataRows readMoreAction callback = go "" Empty
  where
   -- Note that DataRows go in reverse order.
    go :: B.ByteString -> DataRows -> IO ()
    go bs acc
        | B.length bs < headerSize = readMoreAndGo bs acc
        | otherwise = do
            ScanRowResult ch rest r <- scanDataRows bs
            -- We should force accumulator
            let !newAcc = chunk ch acc

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
                    dispatchHeader mt len (B.drop headerSize rest) newAcc

    {-# INLINE dispatchHeader #-}
    dispatchHeader :: Word8 -> Int -> B.ByteString -> DataRows -> IO ()
    dispatchHeader mt len bs acc = case mt of
        -- 'C' - CommandComplete.
        -- Command is completed, return the result.
        67 -> do
            callback $ 
                DataMessage $ reverseDataRows acc

            newBs <- skipBytes bs len
            go newBs Empty

        -- 'I' - EmptyQueryResponse.
        -- PostgreSQL sends this if query string was empty and datarows
        -- should be empty, but anyway we return data collected in `acc`.
        73 -> do
            callback $
                DataMessage $ reverseDataRows acc

            go bs Empty

        -- 'E' - ErrorResponse.
        -- On ErrorResponse we should discard all the collected datarows.
        69 -> do
            (b, newBs) <- readAtLeast bs len
            desc <- eitherToProtocolEx $  parseErrorDesc b
            callback (DataError desc)

            go newBs Empty

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
    readMoreAndGo :: B.ByteString -> DataRows -> IO ()
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

----
-- Decoding 
-----

--  It is better that Decode throws exception on invalid input
{-# INLINABLE decodeOneRow #-}
decodeOneRow :: Decode a -> DataRows -> a
decodeOneRow dec Empty                         = snd $ runDecode dec ""
decodeOneRow dec (DataRows (DataChunk _ bs) _) = snd $ runDecode dec bs

{-# INLINABLE decodeManyRows #-}
decodeManyRows :: Decode a -> DataRows -> V.Vector a
decodeManyRows dec dr = unsafePerformIO $ do
    vec <- MV.unsafeNew . fromIntegral $ countDataRows dr
    let go startInd Empty = pure ()
        go startInd (DataRows (DataChunk len bs) nextDr) = do
            let endInd = startInd + fromIntegral len
            runDecodeIO 
                (traverse_ (writeDec vec) [startInd .. (endInd  -1)]) 
                bs
            go endInd nextDr
    go 0 dr
    V.unsafeFreeze vec
  where
    {-# INLINE writeDec #-}
    writeDec vec pos = dec >>= embedIO . MV.unsafeWrite vec pos

---
-- Utils
-- 

{-# INLINE chunk #-}
chunk :: DataChunk -> DataRows -> DataRows
chunk ch@(DataChunk len bs) dr 
    | len == 0  = dr
    | otherwise = DataRows ch dr

{-# INLINE foldlDataRows #-}
foldlDataRows :: (a -> DataChunk -> a) -> a -> DataRows -> a
foldlDataRows f z = go z
  where
    go a Empty            = a
    go a (DataRows ch dr) = let !na = f a ch in go na dr

{-# INLINE reverseDataRows #-}
reverseDataRows :: DataRows -> DataRows
reverseDataRows = foldlDataRows (flip chunk) Empty

{-# INLINE countDataRows #-}
countDataRows :: DataRows -> Word
countDataRows = foldlDataRows (\acc (DataChunk c _) -> acc + c) 0

-- FIXME delete later
-- | For testing only
{-# INLINE flattenDataRows #-}
flattenDataRows :: DataRows -> B.ByteString
flattenDataRows = foldlDataRows (\acc (DataChunk _ bs) -> acc <> bs) ""
