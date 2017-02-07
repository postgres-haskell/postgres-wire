module Database.PostgreSQL.Protocol.Store.Decode where

import Prelude hiding (takeWhile)
import qualified Data.ByteString as B
import Data.Word
import Data.Int
import Data.Tuple

import Control.Monad
import Control.Applicative

-- Change to Ptr-based parser later
newtype Decode a = Decode
    { runDecode :: B.ByteString -> Either String (B.ByteString, a)}

instance Functor Decode where
    fmap f p = Decode $ fmap (fmap f) . runDecode p
    {-# INLINE fmap #-}

instance Applicative Decode where
    pure x = Decode $ \bs -> Right (bs, x)
    {-# INLINE pure #-}

    p1 <*> p2 = Decode $ \bs -> do
         (bs2, f) <- runDecode p1 bs
         (bs3, x) <- runDecode p2 bs2
         pure (bs3, f x)
    {-# INLINE (<*>) #-}

instance Monad Decode where
    return = pure
    {-# INLINE return #-}

    p >>= f = Decode $ \bs -> do
        (bs2, x) <- runDecode p bs
        runDecode (f x) bs2
    {-# INLINE (>>=) #-}

    fail = Decode . const . Left
    {-# INLINE fail #-}

checkLen :: B.ByteString -> Int -> Either String ()
checkLen bs len | len > B.length bs = Left "too many bytes to read"
                | otherwise = Right ()
{-# INLINE checkLen #-}


takeWhile :: (Word8 -> Bool) -> Decode B.ByteString
takeWhile f = Decode $ \bs -> Right . swap $ B.span f bs
{-# INLINE takeWhile #-}

getByte :: Decode Word8
getByte = Decode $ \bs -> do
    checkLen bs 1
    Right (B.drop 1 bs, B.index bs 0)
{-# INLINE getByte #-}

getTwoBytes :: Decode (Word8, Word8)
getTwoBytes = Decode $ \bs -> do
    checkLen bs 2
    Right (B.drop 2 bs, (B.index bs 0, B.index bs 1))
{-# INLINE getTwoBytes #-}

getFourBytes :: Decode (Word8, Word8, Word8, Word8)
getFourBytes = Decode $ \bs -> do
    checkLen bs 4
    Right (B.drop 4 bs, (B.index bs 0, B.index bs 1, B.index bs 2, B.index bs 3))
{-# INLINE getFourBytes #-}

-----------
-- Public

getByteString :: Int -> Decode B.ByteString
getByteString len = Decode $ \bs -> do
    checkLen bs len
    Right . swap $ B.splitAt len bs
{-# INLINE getByteString #-}

getByteStringNull :: Decode B.ByteString
getByteStringNull = takeWhile (/= 0) <* getWord8
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

