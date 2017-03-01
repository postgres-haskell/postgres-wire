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

data FieldF r a 
    = Single !(FieldDecoder a) 
    | Row !(r a)

{-# INLINE getFieldDec #-}
getFieldDec :: FieldF CompositeValue a -> FieldDecoder a
getFieldDec (Single fd) = fd
getFieldDec (Row r) = composite r

-- High level
--

class PrimField a where

    primField :: RowDecoder r => FieldF r a

    {-# INLINE field #-}
    field :: RowDecoder r => r a
    field = getRowNonNullValue $ getFieldDec primField

    type IsArrayField a :: Bool
    type IsArrayField a = 'False

    type IsNullableField a :: Bool
    type IsNullableField a = 'False

    arrayDim :: Proxy a -> Int
    arrayDim _ = 0

    asArrayData :: V.Vector Int -> Decode a
    asArrayData _ = runRowDecoder (field :: RowValue a)

instance PrimField Int16 where
    primField = Single int2

instance PrimField Int32 where
    primField = Single int4

instance PrimField Int64 where
    primField = Single int8

instance PrimField Bool where
    primField = Single bool

instance PrimField B.ByteString where
    primField = Single getByteString

instance PrimField a => PrimField (Maybe a) where
    primField = undefined

    type IsNullableField (Maybe a) = 'True
    type IsArrayField (Maybe a) = IsArrayField a
    {-# INLINE field #-}
    field = getRowNullValue $ getFieldDec primField

instance (IsAllowedArray (IsNullableField a) (IsArrayField a) ~ 'True, 
          PrimField a)
    => PrimField (V.Vector a) where
    primField = Single $ arrayFieldDecoder 
            (arrayDim (Proxy :: Proxy (V.Vector a)))
            asArrayData 

    type IsArrayField (V.Vector a) = 'True
    arrayDim _ = arrayDim (Proxy :: Proxy a) + 1

    asArrayData vec = V.replicateM (vec V.! arrayDim (Proxy :: Proxy a))
                        $ asArrayData vec

type family IsAllowedArray (n :: Bool) (a :: Bool) :: Bool where
    IsAllowedArray 'True 'True = 'False
    IsAllowedArray _ _ = 'True


-- TODO add array value
newtype RowValue a = RowValue { unRowValue :: Decode a }
    deriving (Functor, Applicative, Monad)
newtype CompositeValue a = CompositeValue { unCompositeValue :: Decode a }
    deriving (Functor, Applicative, Monad)

class (Functor r, Applicative r, Monad r) => RowDecoder r where
    getRowNonNullValue :: FieldDecoder a -> r a
    getRowNullValue :: FieldDecoder a -> r (Maybe a)
    runRowDecoder :: r a -> Decode a

instance RowDecoder RowValue where
    {-# INLINE getRowNonNullValue #-}
    getRowNonNullValue = RowValue . getNonNullable 
    {-# INLINE getRowNullValue #-}
    getRowNullValue = RowValue . getNullable
    {-# INLINE runRowDecoder #-}
    runRowDecoder = unRowValue

instance RowDecoder CompositeValue where
    {-# INLINE getRowNonNullValue #-}
    getRowNonNullValue = CompositeValue 
        . fmap (compositeValue *>) getNonNullable
    {-# INLINE getRowNullValue #-}
    getRowNullValue = CompositeValue
        . fmap (compositeValue *>) getNullable
    {-# INLINE runRowDecoder #-}
    runRowDecoder = unCompositeValue

instance  (PrimField a1, PrimField a2, PrimField a3) 
    => PrimField (a1, a2, a3) where

    {-# INLINE primField #-}
    primField = Row $ (,,) <$> field <*> field <*> field

instance  (PrimField a1, PrimField a2, PrimField a3, PrimField a4,
          PrimField a5, PrimField a6, PrimField a7, PrimField a8,
          PrimField a9, PrimField a10, PrimField a11, PrimField a12)
    => PrimField (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)
  where
    {-# INLINE primField #-}
    primField =  Row $ (,,,,,,,,,,,) <$> field <*> field <*> field <*> field
                           <*> field <*> field <*> field <*> field
                           <*> field <*> field <*> field <*> field


composite :: CompositeValue a -> FieldDecoder a
composite dec _ = compositeHeader *> runRowDecoder dec

{-# INLINE rowDecoder #-}
rowDecoder :: forall a. PrimField a => Decode a
rowDecoder = case primField of
    Single f -> skipDataRowHeader *> runRowDecoder 
                (getRowNonNullValue f :: RowValue a)
    Row r -> skipDataRowHeader *> runRowDecoder (r :: RowValue a)


