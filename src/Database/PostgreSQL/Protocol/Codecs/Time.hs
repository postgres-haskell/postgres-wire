module Database.PostgreSQL.Protocol.Codecs.Time 
    ( dayToPgj
    , utcToMicros
    , localTimeToMicros
    , pgjToDay
    , microsToUTC
    , microsToLocalTime
    , intervalToDiffTime
    , diffTimeToInterval
    ) where

import Data.Int  (Int64, Int32)
import Data.Word (Word32, Word64)
import Data.Time (Day(..), UTCTime(..), LocalTime(..), DiffTime, TimeOfDay,
                  picosecondsToDiffTime, timeToTimeOfDay,
                  diffTimeToPicoseconds, timeOfDayToTime)

{-# INLINE dayToPgj #-}
dayToPgj :: Day -> Integer
dayToPgj = (+ (modifiedJulianEpoch - postgresEpoch)) . toModifiedJulianDay

{-# INLINE utcToMicros #-}
utcToMicros :: UTCTime -> Word32
utcToMicros (UTCTime day diffTime) = fromIntegral $ 
    dayToMcs day + diffTimeToMcs diffTime

{-# INLINE localTimeToMicros #-}
localTimeToMicros :: LocalTime -> Word64
localTimeToMicros (LocalTime day time) = fromIntegral $
    dayToMcs day + timeOfDayToMcs time

{-# INLINE pgjToDay #-}
pgjToDay :: Integral a => a -> Day
pgjToDay = ModifiedJulianDay . fromIntegral 
                        . subtract (modifiedJulianEpoch - postgresEpoch)

{-# INLINE microsToUTC #-}
microsToUTC :: Word64 -> UTCTime
microsToUTC mcs =
    let (d, r) = mcs `divMod` microsInDay
    in UTCTime (pgjToDay d) (mcsToDiffTime r)

{-# INLINE microsToLocalTime #-}
microsToLocalTime :: Word64 -> LocalTime
microsToLocalTime mcs =
    let (d, r) = mcs `divMod` microsInDay
    in LocalTime (pgjToDay d) (mcsToTimeOfDay r)

{-# INLINE intervalToDiffTime #-}
intervalToDiffTime :: Int64 -> Int32 -> Int32 -> DiffTime
intervalToDiffTime mcs days months = picosecondsToDiffTime . mcsToPcs $ 
    microsInDay * (fromIntegral months * daysInMonth + fromIntegral days) 
    + fromIntegral mcs

-- TODO consider adjusted encoding
{-# INLINE diffTimeToInterval #-}
diffTimeToInterval :: DiffTime -> (Int64, Int32, Int32)
diffTimeToInterval dt = (fromIntegral $ diffTimeToMcs dt, 0, 0)

--
-- Utils
--
{-# INLINE dayToMcs #-}
dayToMcs :: Day -> Integer
dayToMcs = (microsInDay *) . dayToPgj 

{-# INLINE diffTimeToMcs #-}
diffTimeToMcs :: DiffTime -> Integer
diffTimeToMcs = pcsToMcs . diffTimeToPicoseconds 

{-# INLINE timeOfDayToMcs #-}
timeOfDayToMcs :: TimeOfDay -> Integer
timeOfDayToMcs = diffTimeToMcs . timeOfDayToTime 

{-# INLINE mcsToDiffTime #-}
mcsToDiffTime :: Integral a => a -> DiffTime
mcsToDiffTime = picosecondsToDiffTime . fromIntegral . mcsToPcs 

{-# INLINE mcsToTimeOfDay #-}
mcsToTimeOfDay :: Integral a => a -> TimeOfDay
mcsToTimeOfDay = timeToTimeOfDay . mcsToDiffTime

{-# INLINE pcsToMcs #-}
pcsToMcs :: Integral a => a -> a
pcsToMcs = (`div` 10 ^ 6)

{-# INLINE mcsToPcs #-}
mcsToPcs :: Integral a => a -> a
mcsToPcs = (* 10 ^ 6)

modifiedJulianEpoch :: Num a => a 
modifiedJulianEpoch = 2400001

postgresEpoch :: Num a => a
postgresEpoch = 2451545

microsInDay :: Num a => a
microsInDay = 24 * 60 * 60 * 10 ^ 6

daysInMonth :: Num a => a
daysInMonth = 30
