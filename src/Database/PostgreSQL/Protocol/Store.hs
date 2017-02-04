module Database.PostgreSQL.Protocol.Store where

import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B(toForeignPtr)
import qualified Data.Vector as V
import Data.Store.Core
import Data.Int (Int16, Int32)
import Data.Word (Word8)
import Foreign
import Data.Monoid
import Data.Foldable


data Encode = Encode Int (Poke ())

instance Monoid Encode where
    mempty = Encode 0 . Poke $ \_ offset -> pure (offset, ())
    (Encode len1 f1) `mappend` (Encode len2 f2) = Encode (len1 + len2) (f1 *> f2)


runEncode :: Encode -> B.ByteString
runEncode (Encode len f) = unsafeEncodeWith f len

fixedPrim :: Int -> (Ptr Word8 -> IO ()) -> Encode
fixedPrim len f = Encode len . Poke $ \state offset -> do
    f $ pokeStatePtr state `plusPtr` offset
    let !newOffset = offset + len
    return (newOffset, ())

putWord8 :: Word8 -> Encode
putWord8 w = fixedPrim 1 $ \p -> poke p w

putWord16BE :: Word16 -> Encode
putWord16BE w = fixedPrim 2 $ \p -> do
    poke p               (fromIntegral (shiftR w 8) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral w            :: Word8)

putWord32BE :: Word32 -> Encode
putWord32BE w = fixedPrim 4 $ \p -> do
    poke p               (fromIntegral (shiftR w 24) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (shiftR w 16) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (shiftR w  8) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral w             :: Word8)

putInt32BE :: Int32 -> Encode
putInt32BE = putWord32BE . fromIntegral

putInt16BE :: Int16 -> Encode
putInt16BE = putWord16BE . fromIntegral

putByteString :: B.ByteString -> Encode
putByteString bs =
    let (ptr, offset, len) = B.toForeignPtr bs
    in Encode len $ pokeFromForeignPtr ptr offset len

-- | C-like string
putPgString :: B.ByteString -> Encode
putPgString bs = putByteString bs <> putWord8 0

-- | List with prepended length
putPgList :: V.Vector Encode -> Encode
putPgList v = putInt16BE (fromIntegral $ V.length v) <> fold v

