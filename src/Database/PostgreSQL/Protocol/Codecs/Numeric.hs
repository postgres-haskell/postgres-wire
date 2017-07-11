{-# language LambdaCase #-}
module Database.PostgreSQL.Protocol.Codecs.Numeric where

-- TODO test it
import Data.Tuple
import Data.Word
import Data.Int
import Data.Foldable
import Data.Scientific
import Data.List (unfoldr)

integerToDigits :: Integer -> [Word16]
integerToDigits = (reverse.) . unfoldr $ \case 
    0 -> Nothing
    n -> let (rest, rem) =  n `divMod` nBase in Just (fromIntegral rem, rest)

toNumericSign :: Scientific -> Word16
toNumericSign s | s >= 0 = 0x0000
              | otherwise = 0x4000

scientificToNumeric :: Scientific -> (Int16, Word16, [Word16])
scientificToNumeric number = 
    let a       = base10Exponent number `mod` nBaseDigits
        adjExp  = base10Exponent number - a
        adjCoef = coefficient number * (10 ^ a)
        digits  = integerToDigits $ abs adjCoef
        weight  = fromIntegral $ length digits + adjExp `div` nBaseDigits - 1
        scale   = fromIntegral . negate $ min (base10Exponent number) 0
    in (weight, scale, digits)

digitsToInteger :: [Word16] -> Integer
digitsToInteger = foldl' (\acc n -> acc * nBase + fromIntegral n) 0

fromNumericSign :: (Monad m, Num a) => Word16 -> m a
fromNumericSign 0x0000 = pure 1
fromNumericSign 0x4000 = pure $ -1
-- NaN code is 0xC000, it is not supported.
fromNumericSign _      = fail "Unknown numeric sign"  

numericToScientific :: Integer -> Int16 -> [Word16] -> Scientific
numericToScientific sign weight digits = 
    let coef = digitsToInteger digits * sign
        exp' = (fromIntegral weight + 1 - length digits) * nBaseDigits
    in scientific coef exp' 

nBase :: Num a => a
nBase = 10000

nBaseDigits :: Num a => a
nBaseDigits = 4

