module Database.PostgreSQL.Protocol.Codecs.Numeric where

-- TODO test it
import Data.Word
import Data.Int
import Data.Foldable
import Data.Fixed

numericDigit :: [Word16] -> Integer
numericDigit = foldl' (\acc n -> acc * nBase + fromIntegral n) 0

numericSign :: Num a => Word16 -> Maybe a
numericSign 0x0000 = Just 1
numericSign 0x4000 = Just $ -1
numericSign _      = Nothing  -- NaN code is 0xC000, it is not supported.

fixedFromNumeric :: HasResolution a => Int16 -> [Word16] -> Fixed a
fixedFromNumeric weight digits = undefined

nBase :: Num a => a
nBase = 10000
