module Database.PostgreSQL.Protocol.Codecs.Encoders 
    ( bool
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
    , timestamp
    , timestamptz
    , uuid
    ) where

import Data.ByteString  (ByteString)
import Data.Char        (ord)
import Data.Int         (Int16, Int32, Int64)
import Data.Monoid      ((<>))
import Data.Scientific  (Scientific)
import Data.Time        (Day, UTCTime, LocalTime, DiffTime, TimeOfDay)
import Data.UUID        (UUID, toWords)

import Database.PostgreSQL.Protocol.Store.Encode
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Codecs.Time
import Database.PostgreSQL.Protocol.Codecs.Numeric

--
-- Primitives
--

{-# INLINE bool #-}
bool :: Bool -> Encode
bool False = putWord8 0
bool True  = putWord8 1

{-# INLINE bytea #-}
bytea :: ByteString -> Encode
bytea = putByteString

{-# INLINE char #-}
char :: Char -> Encode
char c
  | ord(c) >= 128 = error "Character code must be below 128"
  | otherwise = (putWord8 . fromIntegral . ord) c

{-# INLINE date #-}
date :: Day -> Encode
date = putInt32BE . dayToPgj

{-# INLINE float4 #-}
float4 :: Float -> Encode
float4 = putFloat32BE

{-# INLINE float8 #-}
float8 :: Double -> Encode
float8 = putFloat64BE

{-# INLINE int2 #-}
int2 :: Int16 -> Encode
int2 = putInt16BE

{-# INLINE int4 #-}
int4 :: Int32 -> Encode
int4 = putInt32BE 

{-# INLINE int8 #-}
int8 :: Int64 -> Encode
int8 = putInt64BE 

{-# INLINE interval #-}
interval :: DiffTime -> Encode
interval v = let (mcs, days, months) = diffTimeToInterval v 
             in putInt64BE mcs <> putInt32BE days <> putInt32BE months

-- | Encodes representation of JSON as @ByteString@.
{-# INLINE bsJsonText #-}
bsJsonText :: ByteString -> Encode
bsJsonText = putByteString

-- | Encodes representation of JSONB as @ByteString@.
{-# INLINE bsJsonBytes #-}
bsJsonBytes :: ByteString -> Encode
bsJsonBytes bs = putWord8 1 <> putByteString bs

{-# INLINE numeric #-}
numeric :: Scientific -> Encode
numeric n = 
    let (count, weight, scale, digits) = scientificToNumeric n
    in    putWord16BE count
       <> putInt16BE weight
       <> putWord16BE (toNumericSign n)
       <> putWord16BE scale
       <> foldMap putWord16BE digits

-- | Encodes text.
{-# INLINE bsText #-}
bsText :: ByteString -> Encode
bsText = putByteString

{-# INLINE time #-}
time :: TimeOfDay -> Encode
time = putInt64BE . timeOfDayToMcs

{-# INLINE timestamp #-}
timestamp :: LocalTime -> Encode
timestamp = putInt64BE . localTimeToMicros 

{-# INLINE timestamptz #-}
timestamptz :: UTCTime -> Encode
timestamptz = putInt64BE . utcToMicros 

{-# INLINE uuid #-}
uuid :: UUID -> Encode
uuid v = let (a, b, c, d) = toWords v 
         in putWord32BE a <> putWord32BE b <> putWord32BE c <> putWord32BE d
