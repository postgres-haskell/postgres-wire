{-# language ForeignFunctionInterface #-}
module Database.PostgreSQL.Protocol.Utils where

import Foreign.C.Types          (CInt, CSize(..), CChar)
import Foreign (Ptr, peek, alloca)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B

data ScanRowResult = ScanRowResult
    {-# UNPACK #-} !B.ByteString  -- chunk of datarows, may be empty
    {-# UNPACK #-} !B.ByteString  -- the rest of string
    {-# UNPACK #-} !Int           -- reason code

{-# INLINE scanDataRows #-}
-- | Scans `ByteString` for a chunk of `DataRow`s.
scanDataRows :: B.ByteString -> IO ScanRowResult
scanDataRows bs =
    alloca $ \reasonPtr ->
        B.unsafeUseAsCStringLen bs $ \(ptr, len) -> do
            offset <- fromIntegral <$>
                        c_scan_datarows ptr (fromIntegral len) reasonPtr
            reason <- peek reasonPtr
            let (ch, rest) = B.splitAt offset bs
            pure $ ScanRowResult ch rest $ fromIntegral reason

foreign import ccall unsafe "static pw_utils.h scan_datarows" c_scan_datarows
    :: Ptr CChar -> CSize -> Ptr CInt -> IO CSize

