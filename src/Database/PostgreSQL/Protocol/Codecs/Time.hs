module Database.PostgreSQL.Protocol.Codecs.Time 
    ( dayToPgj
    , utcToMicros
    , localTimeToMicros
    , pgjToDay
    , microsToUTC
    , microsToLocalTime
    ) where

import Data.Int  (Int64)
import Data.Time (Day(..), UTCTime(..), LocalTime(..), DiffTime, TimeOfDay,
                  picosecondsToDiffTime, timeToTimeOfDay,
                  diffTimeToPicoseconds, timeOfDayToTime)

{-# INLINE dayToPgj #-}
dayToPgj :: Day -> Integer
dayToPgj = (+ (modifiedJulianEpoch - postgresEpoch)) . toModifiedJulianDay

{-# INLINE utcToMicros #-}
utcToMicros :: UTCTime -> Int64
utcToMicros (UTCTime day diffTime) = fromIntegral $ 
    dayToMcs day + diffTimeToMcs diffTime

{-# INLINE localTimeToMicros #-}
localTimeToMicros :: LocalTime -> Int64
localTimeToMicros (LocalTime day time) = fromIntegral $
    dayToMcs day + timeOfDayToMcs time

{-# INLINE pgjToDay #-}
pgjToDay :: Integral a => a -> Day
pgjToDay = ModifiedJulianDay . fromIntegral 
                        . subtract (modifiedJulianEpoch - postgresEpoch)

{-# INLINE microsToUTC #-}
microsToUTC :: Int64 -> UTCTime
microsToUTC mcs =
    let (d, r) = mcs `divMod` microsInDay
    in UTCTime (pgjToDay d) (mcsToDiffTime r)

{-# INLINE microsToLocalTime #-}
microsToLocalTime :: Int64 -> LocalTime
microsToLocalTime mcs =
    let (d, r) = mcs `divMod` microsInDay
    in LocalTime (pgjToDay d) (mcsToTimeOfDay r)

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
