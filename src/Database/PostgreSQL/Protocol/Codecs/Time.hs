module Database.PostgreSQL.Protocol.Codecs.Time where

import Data.Time

modifiedJulianEpoch :: Num a => a 
modifiedJulianEpoch = 2400001

postgresEpoch :: Num a => a
postgresEpoch = 2451545

microsInDay :: Num a => a
microsInDay = 24 * 60 * 60 * 10 ^ 6

picosecondsToMicros :: Integral a => a -> a
picosecondsToMicros = (`div` 10 ^ 6)

dayToPostgresJulian :: Day -> Integer
dayToPostgresJulian = (+ (modifiedJulianEpoch - postgresEpoch)) 
                        . toModifiedJulianDay

postgresJulianToDay :: Integral a => a -> Day
postgresJulianToDay = ModifiedJulianDay . fromIntegral 
                        . subtract (modifiedJulianEpoch - postgresEpoch)

utcToMicros :: UTCTime -> Int64
utcToMicros (UTCTime day diffTime) =
    let d = microsInDay $ dayToPostgresJulian day
        p = picosecondsToMicros $ diffTimeToPicoseconds diffTime
    in fromIntegral $ d + p

localTimeToMicros :: LocalTime -> Int64
localTimeToMicros (LocalTime day time) =
  let d = microsInDay $ dayToPostgresJulian day
      p = picosecondsToMicros . diffTimeToPicoseconds $ timeOfDayToTime timeX
      in fromIntegral $ d + p
