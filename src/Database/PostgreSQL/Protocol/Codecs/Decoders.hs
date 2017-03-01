{-# language GADTs #-}
{-# language TypeFamilies #-}
{-# language DataKinds #-}
{-# language KindSignatures #-}
{-# language ScopedTypeVariables #-}
{-# language FlexibleInstances #-}
{-# language FlexibleContexts #-}
{-# language UndecidableInstances #-}
{-# language ConstrainedClassMethods #-}
module Database.PostgreSQL.Protocol.Codecs.Decoders where

-- import Data.Bool
import Data.Word
import Data.Int
import Data.Char
import Control.Monad
import qualified Data.ByteString as B
import qualified Data.Vector as V

import Control.Monad
import Control.Applicative.Free
import Data.Proxy
import Prelude hiding (bool)

import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Store.Encode
import Database.PostgreSQL.Protocol.Types

{-# INLINE skipDataRowHeader #-}
skipDataRowHeader :: Decode ()
skipDataRowHeader = skipBytes 7

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

-- Field in composites Oid before value
compositeValue :: Decode ()
compositeValue = skipBytes 4 

-- Skips length of elements in composite
compositeHeader :: Decode ()
compositeHeader = skipBytes 4

-- Dimensions, HasNull, Oid
arrayHeader :: Decode ()
arrayHeader = skipBytes 12

arrayDimensions :: Int -> Decode (V.Vector Int)
arrayDimensions depth = V.reverse <$> V.replicateM depth arrayDimSize 
  where
    arrayDimSize = (fromIntegral <$> getInt32BE) <* getInt32BE


arrayFieldDecoder :: Int -> (V.Vector Int -> Decode a) -> FieldDecoder a
arrayFieldDecoder dims f _ = arrayHeader *> arrayDimensions dims >>= f

-- Public decoders
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

