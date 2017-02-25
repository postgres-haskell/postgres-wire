module Database.PostgreSQL.Protocol.Codecs.Decoders where

import Data.Bool
import Data.Word
import Data.Int
import Data.Char
import Control.Monad
import qualified Data.ByteString as B
import qualified Data.Vector as V

import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.Store.Encode
import Database.PostgreSQL.Protocol.Types

skipDataRowHeader :: Decode ()
skipDataRowHeader = skipBytes 7

fieldLength :: Decode Int
fieldLength = fromIntegral <$> getInt32BE

getNonNullable :: FieldDecoder a -> Decode a
getNonNullable dec = fieldLength >>= runFieldDecoder dec

getNullable :: FieldDecoder a -> Decode (Maybe a)
getNullable dec = do
    len <- fieldLength
    if len == -1
    then pure Nothing
    else Just <$!> runFieldDecoder dec len

-- Field in composites Oid before value
compositeValue :: Decode a -> Decode a
compositeValue dec = skipBytes 4 >> dec

compositeHeader :: Decode ()
compositeHeader = skipBytes 4

arrayData :: Int -> Decode a -> Decode (V.Vector a)
arrayData len dec = undefined

-- Public decoders
-- | Decodes only content of a field.
newtype FieldDecoder a = FieldDecoder { runFieldDecoder :: Int -> Decode a }

int2 :: FieldDecoder Int16
int2 = FieldDecoder $ \ _ -> getInt16BE

int4 :: FieldDecoder Int32
int4 = FieldDecoder $ \ _ -> getInt32BE 

int8 :: FieldDecoder Int64
int8 = FieldDecoder $ \ _ -> getInt64BE 

bool :: FieldDecoder Bool
bool = FieldDecoder $ \ _ -> (== 1) <$> getWord8
