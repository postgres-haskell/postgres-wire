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

runDecode :: Decode a -> B.ByteString -> Either String (B.ByteString, a)
runDecode (Decode dec) bs =
    let (offset,v ) = decodeExPortionWith dec bs
    in Right (B.drop offset bs, v)
{-# INLINE runDecode #-}

fixed :: Int -> (Ptr Word8 -> IO a) -> Decode a
fixed len f = Decode . Peek $ \ps ptr -> do
    !v <- f ptr
    let !newPtr = ptr `plusPtr` len
    return (newPtr, v)
{-# INLINE fixed #-}

getByte :: Decode Word8
getByte = fixed 1 peek
{-# INLINE getByte #-}

getTwoBytes :: Decode (Word8, Word8)
getTwoBytes = fixed 2 $ \ptr -> do
    b1 <- peek ptr
    b2 <- peekByteOff ptr 1
    return (b1, b2)
{-# INLINE getTwoBytes #-}

getFourBytes :: Decode (Word8, Word8, Word8, Word8)
getFourBytes = fixed 4 $ \ptr -> do
    b1 <- peek ptr
    b2 <- peekByteOff ptr 1
    b3 <- peekByteOff ptr 2
    b4 <- peekByteOff ptr 3
    return (b1, b2, b3, b4)
{-# INLINE getFourBytes #-}

-----------
-- Public

getByteString :: Int -> Decode B.ByteString
getByteString len = Decode . Peek $ \ps ptr -> do
    bs <- B.packCStringLen (castPtr ptr, len)
    let !newPtr = ptr `plusPtr` len
    return (newPtr, bs)
{-# INLINE getByteString #-}

getByteStringNull :: Decode B.ByteString
getByteStringNull = Decode . Peek $ \ps ptr -> do
    bs <- B.packCString (castPtr ptr)
    let !newPtr = ptr `plusPtr` (B.length bs + 1)
    return (newPtr, bs)
{-# INLINE getByteStringNull #-}

getWord8 :: Decode Word8
getWord8 = getByte
{-# INLINE getWord8 #-}

getWord16BE :: Decode Word16
getWord16BE = do
    (w1, w2) <- getTwoBytes
    pure $ fromIntegral w1 * 256 +
           fromIntegral w2
{-# INLINE getWord16BE #-}

getWord32BE :: Decode Word32
getWord32BE = do
    (w1, w2, w3, w4) <- getFourBytes
    pure $ fromIntegral w1 * 256 *256 *256 +
           fromIntegral w2 * 256 *256 +
           fromIntegral w3 * 256 +
           fromIntegral w4
{-# INLINE getWord32BE #-}

getInt16BE :: Decode Int16
getInt16BE = fromIntegral <$> getWord16BE
{-# INLINE getInt16BE #-}

getInt32BE :: Decode Int32
getInt32BE = fromIntegral <$> getWord32BE
{-# INLINE getInt32BE #-}

