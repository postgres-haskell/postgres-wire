module Database.PostgreSQL.Protocol.Codecs.Decoders 
    ( dataRowHeader
    , getNonNullable
    , getNullable
    , FieldDecoder
    , bool
    , bytea
    , char
    , date
    , float4
    , float8
    , int2
    , int4
    , int8
    , interval
    , bsJsonText
    , bsJsonBytes
    , numeric
    , bsText
    , time
    , timetz
    , timestamp
    , timestamptz
    , uuid
    ) where

import Prelude hiding   (bool)
import Control.Monad    (replicateM, (<$!>))
import Data.ByteString  (ByteString)
import Data.Char        (chr)
import Data.Int         (Int16, Int32, Int64)
import Data.Scientific  (Scientific)
import Data.Time        (Day, UTCTime, LocalTime, DiffTime, TimeOfDay)
import Data.UUID        (UUID, fromWords)
import qualified Data.Vector as V

import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Codecs.Time
import Database.PostgreSQL.Protocol.Codecs.Numeric

-- | Decodes DataRow header.
-- 2 bytes - count of columns in the DataRow
{-# INLINE dataRowHeader #-}
dataRowHeader :: Decode ()
dataRowHeader = skipBytes 2

{-# INLINE fieldLength #-}
fieldLength :: Decode Int
fieldLength = fromIntegral <$> getWord32BE

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
    -- 4 bytes - count of elements in the dimension
    -- 4 bytes - lower bound
    arrayDimSize = (fromIntegral <$> getWord32BE) <* getWord32BE

{-# INLINE arrayFieldDecoder #-}
arrayFieldDecoder :: Int -> (V.Vector Int -> Decode a) -> FieldDecoder a
arrayFieldDecoder dims f _ = arrayHeader *> arrayDimensions dims >>= f

--
-- Primitives
--

-- | Decodes only a content of the field.
type FieldDecoder a = Int -> Decode a 

{-# INLINE bool #-}
bool :: FieldDecoder Bool
bool _ = (== 1) <$> getWord8

{-# INLINE bytea #-}
bytea :: FieldDecoder ByteString
bytea = getByteString

{-# INLINE char #-}
char :: FieldDecoder Char
char _ = chr . fromIntegral <$> getWord8

{-# INLINE date #-}
date :: FieldDecoder Day
date _ = pgjToDay <$> getInt32BE

{-# INLINE float4 #-}
float4 :: FieldDecoder Float
float4 _ = getFloat32BE

{-# INLINE float8 #-}
float8 :: FieldDecoder Double
float8 _ = getFloat64BE

{-# INLINE int2 #-}
int2 :: FieldDecoder Int16
int2 _ = getInt16BE

{-# INLINE int4 #-}
int4 :: FieldDecoder Int32
int4 _ = getInt32BE 

{-# INLINE int8 #-}
int8 :: FieldDecoder Int64
int8 _ = getInt64BE 

{-# INLINE interval #-}
interval :: FieldDecoder DiffTime
interval _ = intervalToDiffTime <$> getInt64BE <*> getInt32BE <*> getInt32BE

-- | Decodes representation of JSON as @ByteString@.
{-# INLINE bsJsonText #-}
bsJsonText :: FieldDecoder ByteString
bsJsonText = getByteString

-- | Decodes representation of JSONB as @ByteString@.
{-# INLINE bsJsonBytes #-}
bsJsonBytes :: FieldDecoder ByteString
bsJsonBytes len = getWord8 *> getByteString (len - 1)

{-# INLINE numeric #-}
numeric :: FieldDecoder Scientific
numeric _ = do 
    ndigits <- getWord16BE
    weight  <- getInt16BE
    sign    <- fromNumericSign =<< getWord16BE 
    _       <- getWord16BE
    numericToScientific sign weight <$> 
        replicateM (fromIntegral ndigits) getWord16BE

-- | Decodes text without applying encoding.
{-# INLINE bsText #-}
bsText :: FieldDecoder ByteString
bsText = getByteString

{-# INLINE time #-}
time :: FieldDecoder TimeOfDay
time _ = mcsToTimeOfDay <$> getInt64BE

{-# INLINE timetz #-}
timetz :: FieldDecoder TimeOfDay
timetz _ = do
  t <- getInt64BE
  skipBytes 4
  return $ mcsToTimeOfDay t

{-# INLINE timestamp #-}
timestamp :: FieldDecoder LocalTime
timestamp _ = microsToLocalTime <$> getInt64BE

{-# INLINE timestamptz #-}
timestamptz :: FieldDecoder UTCTime
timestamptz _ = microsToUTC <$> getInt64BE

{-# INLINE uuid #-}
uuid :: FieldDecoder UUID
uuid _ = fromWords 
    <$> getWord32BE
    <*> getWord32BE
    <*> getWord32BE
    <*> getWord32BE
