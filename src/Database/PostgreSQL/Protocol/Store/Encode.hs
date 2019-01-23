module Database.PostgreSQL.Protocol.Store.Encode 
    ( Encode
    , getEncodeLen
    , runEncode
    , putByteString
    , putByteStringNull
    , putWord8
    , putWord16BE
    , putWord32BE
    , putWord64BE
    , putInt16BE
    , putInt32BE
    , putInt64BE
    , putFloat32BE
    , putFloat64BE
    ) where

import Data.Monoid  (Monoid(..), (<>))
import Foreign      (Storable, alloca, peek, poke, castPtr, plusPtr, Ptr)
import Data.Int     (Int16, Int32, Int64)
import Data.Word    (Word8, Word16, Word32, Word64, 
                     byteSwap16, byteSwap32, byteSwap64)

import Data.ByteString          (ByteString)
import Data.ByteString.Internal (toForeignPtr)
import Data.Store.Core          (Poke(..), unsafeEncodeWith, pokeStatePtr,
                                 pokeFromForeignPtr)

data Encode = Encode {-# UNPACK #-} !Int !(Poke ())

instance Semigroup Encode where
  {-# INLINE (<>) #-}
  (Encode len1 f1) <> (Encode len2 f2) = Encode (len1 + len2) (f1 *> f2)

instance Monoid Encode where
    {-# INLINE mempty #-}
    mempty = Encode 0 . Poke $ \_ offset -> pure (offset, ())

instance Show Encode where
    show (Encode len _) = "Encode instance of length " ++ show len

{-# INLINE getEncodeLen #-}
getEncodeLen :: Encode -> Int
getEncodeLen (Encode len _) = len

{-# INLINE runEncode #-}
runEncode :: Encode -> ByteString
runEncode (Encode len f) = unsafeEncodeWith f len

{-# INLINE fixed #-}
fixed :: Int -> (Ptr Word8 -> IO ()) -> Encode
fixed len f = Encode len . Poke $ \state offset -> do
    f $ pokeStatePtr state `plusPtr` offset
    let !newOffset = offset + len
    return (newOffset, ())

{-# INLINE putByteString #-}
putByteString :: ByteString -> Encode
putByteString bs =
    let (ptr, offset, len) = toForeignPtr bs
    in Encode len $ pokeFromForeignPtr ptr offset len

-- | C-like string
{-# INLINE putByteStringNull #-}
putByteStringNull :: ByteString -> Encode
putByteStringNull bs = putByteString bs <> putWord8 0

{-# INLINE putWord8 #-}
putWord8 :: Word8 -> Encode
putWord8 w = fixed 1 $ \p -> poke p w

{-# INLINE putWord16BE #-}
putWord16BE :: Word16 -> Encode
putWord16BE w = fixed 2 $ \p -> poke (castPtr p) (byteSwap16 w)

{-# INLINE putWord32BE #-}
putWord32BE :: Word32 -> Encode
putWord32BE w = fixed 4 $ \p -> poke (castPtr p) (byteSwap32 w)

{-# INLINE putWord64BE #-}
putWord64BE :: Word64 -> Encode
putWord64BE w = fixed 8 $ \p -> poke (castPtr p) (byteSwap64 w)

{-# INLINE putInt16BE #-}
putInt16BE :: Int16 -> Encode
putInt16BE = putWord16BE . fromIntegral

{-# INLINE putInt32BE #-}
putInt32BE :: Int32 -> Encode
putInt32BE = putWord32BE . fromIntegral

{-# INLINE putInt64BE #-}
putInt64BE :: Int64 -> Encode
putInt64BE = putWord64BE . fromIntegral

{-# INLINE putFloat32BE #-}
putFloat32BE :: Float -> Encode
putFloat32BE float = fixed 4 $ \ptr -> byteSwap32 <$> floatToWord float
                                        >>= poke (castPtr ptr)

{-# INLINE putFloat64BE #-}
putFloat64BE :: Double -> Encode
putFloat64BE double = fixed 8 $ \ptr -> byteSwap64 <$> floatToWord double
                                        >>= poke (castPtr ptr)

{-# INLINE floatToWord #-}
floatToWord :: (Storable word, Storable float) => float -> IO word
floatToWord float = alloca $ \buf -> do
    poke (castPtr buf) float
    peek buf
