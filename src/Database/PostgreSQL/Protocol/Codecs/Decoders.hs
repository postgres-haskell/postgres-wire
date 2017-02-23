module Database.PostgreSQL.Protocol.Codecs.Decoders where

import Data.Word
import Data.Int
import Data.Char
import Control.Monad
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Store.Decode

{-# INLINE skipHeader #-}
skipHeader :: Decode ()
skipHeader = skipBytes 7

{-# INLINE getNullable #-}
getNullable :: Decode a -> Decode (Maybe a)
getNullable dec = do
    len <- getInt32BE
    if len == -1
    then pure Nothing
    else Just <$!> dec

{-# INLINE getString #-}
getString :: Decode (Maybe B.ByteString)
getString = getInt32BE >>= (Just <$!>) . getByteString . fromIntegral

{-# INLINE getBool #-}
getBool :: Decode Bool
getBool = (== 1) <$> getWord8

{-# INLINE getCh #-}
getCh :: Decode Char
getCh = (chr . fromIntegral) <$> getWord8


getCustom :: Decode (Maybe B.ByteString, Maybe Int32, Maybe Int32, 
                     Maybe Int16, Maybe Bool, Maybe Char, Maybe Bool, 
                     Maybe Bool, Maybe Char, Maybe Int32, Maybe Int32, 
                     Maybe Int32)
getCustom = (,,,,,,,,,,,) <$> 
    getString <*>
    (getNullable getInt32BE) <*>
    (getNullable getInt32BE) <*>
    (getNullable getInt16BE) <*>
    (getNullable getBool) <*>
    (getNullable getCh) <*>
    (getNullable getBool) <*>
    (getNullable getBool) <*>
    (getNullable getCh) <*>
    (getNullable getInt32BE) <*>
    (getNullable getInt32BE) <*>
    (getNullable getInt32BE) 

getCustomRow = skipHeader *> getCustom

