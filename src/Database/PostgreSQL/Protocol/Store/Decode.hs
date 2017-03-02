module Database.PostgreSQL.Protocol.Store.Decode where

import Prelude hiding (takeWhile)
import qualified Data.ByteString as B
import Data.Word
import Data.Int
import Data.Tuple

import Data.Store.Core

import Foreign
import Control.Monad
import Control.Applicative
import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as B

newtype Decode a = Decode (Peek a)
    deriving (Functor, Applicative, Monad)

{-# INLINE runDecode #-}
runDecode :: Decode a -> B.ByteString -> (B.ByteString, a)
runDecode (Decode dec) bs =
    let (offset,v ) = decodeExPortionWith dec bs
    in (B.drop offset bs, v)

{-# INLINE runDecodeIO #-}
runDecodeIO :: Decode a -> B.ByteString -> IO (B.ByteString, a)
runDecodeIO (Decode dec) bs = do
    (offset, v) <- decodeIOPortionWith dec bs
    pure (B.drop offset bs, v)

{-# INLINE embedIO #-}
embedIO :: IO a -> Decode a
embedIO action = Decode $ Peek $ \_ ptr -> do
    v <- action
    return (ptr, v)

{-# INLINE prim #-}
prim :: Int -> (Ptr Word8 -> IO a) -> Decode a
prim len f = Decode $ Peek $ \ps ptr -> do
    !v <- f ptr
    let !newPtr = ptr `plusPtr` len
    return (newPtr, v)
    -- return $ PeekResult newPtr v

-- Public

{-# INLINE skipBytes #-}
skipBytes :: Int -> Decode ()
skipBytes n = prim n $ const $ pure ()

{-# INLINE getByteString #-}
getByteString :: Int -> Decode B.ByteString
getByteString len = Decode $ Peek $ \ps ptr -> do
    bs <- B.packCStringLen (castPtr ptr, len)
    let !newPtr = ptr `plusPtr` len
    -- return $ PeekResult newPtr bs
    return (newPtr, bs)

{-# INLINE getByteStringNull #-}
getByteStringNull :: Decode B.ByteString
getByteStringNull = Decode $ Peek $ \ps ptr -> do
    bs <- B.packCString (castPtr ptr)
    let !newPtr = ptr `plusPtr` (B.length bs + 1)
    -- return $ PeekResult newPtr bs
    return (newPtr, bs)

{-# INLINE getWord8 #-}
getWord8 :: Decode Word8
getWord8 = prim 1 peek

{-# INLINE getWord16BE #-}
getWord16BE :: Decode Word16
getWord16BE = prim 2 $ \ptr -> byteSwap16 <$> peek (castPtr ptr)

{-# INLINE getWord32BE #-}
getWord32BE :: Decode Word32
getWord32BE = prim 4 $ \ptr -> byteSwap32 <$> peek (castPtr ptr)

{-# INLINE getWord64BE #-}
getWord64BE :: Decode Word64
getWord64BE = prim 8 $ \ptr -> byteSwap64 <$> peek (castPtr ptr)

{-# INLINE getInt16BE #-}
getInt16BE :: Decode Int16
getInt16BE = fromIntegral <$> getWord16BE

{-# INLINE getInt32BE #-}
getInt32BE :: Decode Int32
getInt32BE = fromIntegral <$> getWord32BE

{-# INLINE getInt64BE #-}
getInt64BE :: Decode Int64
getInt64BE = fromIntegral <$> getWord64BE

{-# INLINE getFloat32BE #-}
getFloat32BE :: Decode Float
getFloat32BE = prim 4 $ \ptr -> byteSwap32 <$> peek (castPtr ptr) 
                                    >>= wordToFloat

{-# INLINE getFloat64BE #-}
getFloat64BE :: Decode Double
getFloat64BE = prim 8 $ \ptr -> byteSwap64 <$> peek (castPtr ptr) 
                                    >>= wordToFloat

{-# INLINE wordToFloat #-}
wordToFloat :: (Storable word, Storable float) => word -> IO float
wordToFloat word = alloca $ \buf -> do
    poke (castPtr buf) word
    peek buf
