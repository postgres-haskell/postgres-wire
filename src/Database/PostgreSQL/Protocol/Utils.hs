{-# language ForeignFunctionInterface #-}
module Database.PostgreSQL.Protocol.Utils where

import Foreign.C.Types          (CInt, CSize(..), CChar, CULong)
import Foreign                  (Ptr, peek, alloca)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B

import Database.PostgreSQL.Protocol.Types (DataChunk(..))

data ScanRowResult = ScanRowResult
    {-# UNPACK #-} !DataChunk     -- chunk of datarows, may be empty
    {-# UNPACK #-} !B.ByteString  -- the rest of string
    {-# UNPACK #-} !Int           -- reason code

-- | Scans `ByteString` for a chunk of `DataRow`s.
{-# INLINE scanDataRows #-}
scanDataRows :: B.ByteString -> IO ScanRowResult
scanDataRows bs =
    alloca $ \countPtr -> 
        alloca $ \reasonPtr ->
            B.unsafeUseAsCStringLen bs $ \(ptr, len) -> do
                offset <- fromIntegral <$>
                    c_scan_datarows ptr (fromIntegral len) countPtr reasonPtr
                reason <- fromIntegral <$> peek reasonPtr
                count  <- fromIntegral <$> peek countPtr
                let (ch, rest) = B.splitAt offset bs
                pure $ ScanRowResult (DataChunk count ch) rest reason

foreign import ccall unsafe "static pw_utils.h scan_datarows" c_scan_datarows
    :: Ptr CChar -> CSize -> Ptr CULong -> Ptr CInt -> IO CSize

