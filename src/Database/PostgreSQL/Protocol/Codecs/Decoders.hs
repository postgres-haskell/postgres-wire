module Database.PostgreSQL.Protocol.Codecs.Decoders where

import Data.Word
import Data.Int
import Data.Char
import qualified Data.ByteString as B
import qualified Data.Vector as V

import Control.Monad
import Prelude hiding (bool)

import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Types

-- | Decodes DataRow header.
-- 1 byte - Message Header
-- 4 bytes - Message length
-- 2 bytes - count of columns in the DataRow
{-# INLINE dataRowHeader #-}
dataRowHeader :: Decode ()
dataRowHeader  = skipBytes 7

{-# INLINE fieldLength #-}
fieldLength :: Decode Int
fieldLength = fromIntegral <$> getInt32BE

{-# INLINE getNonNullable #-}
getNonNullable :: FieldDecoder a -> Decode a
getNonNullable fdec = fieldLength >>= fdec

{-# INLINE getNullable #-}
getNullable :: FieldDecoder a -> Decode (Maybe a)
getNullable fdec = do
    len <- fieldLength
    if len == -1
    then pure Nothing
    else Just <$!> fdec len

-- | Field in composites contain Oid before value
{-# INLINE compositeValuePrefix #-}
compositeValuePrefix :: Decode ()
compositeValuePrefix = skipBytes 4 

-- | Skips length of elements in composite
{-# INLINE compositeHeader #-}
compositeHeader :: Decode ()
compositeHeader = skipBytes 4

-- | Skips array header.
-- 4 bytes - count of dimensions
-- 4 bytes - if array contains any NULL
-- 4 bytes - element Oid
{-# INLINE arrayHeader #-}
arrayHeader :: Decode ()
arrayHeader = skipBytes 12

-- | Decodes size of each dimension.
{-# INLINE arrayDimensions #-}
arrayDimensions :: Int -> Decode (V.Vector Int)
arrayDimensions dims = V.reverse <$> V.replicateM dims arrayDimSize 
  where
    -- 4 bytes - count of elements in dimension
    -- 4 bytes - lower bound
    arrayDimSize = (fromIntegral <$> getInt32BE) <* getInt32BE

{-# INLINE arrayFieldDecoder #-}
arrayFieldDecoder :: Int -> (V.Vector Int -> Decode a) -> FieldDecoder a
arrayFieldDecoder dims f _ = arrayHeader *> arrayDimensions dims >>= f

-- | Decodes only content of a field.
type FieldDecoder a = Int -> Decode a 

{-# INLINE int2 #-}
int2 :: FieldDecoder Int16
int2 _ =  getInt16BE

{-# INLINE int4 #-}
int4 :: FieldDecoder Int32
int4 _ =  getInt32BE 

{-# INLINE int8 #-}
int8 :: FieldDecoder Int64
int8 _ =  getInt64BE 

{-# INLINE bool #-}
bool :: FieldDecoder Bool
bool _ = (== 1) <$> getWord8

