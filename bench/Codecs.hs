module Main where

import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString (ByteString)
import Data.Monoid
import Data.Foldable
import System.IO.Unsafe
import Data.Vector as V(fromList, empty)
import Criterion.Main
import Data.Time
import Data.UUID
import Data.UUID.V4 (nextRandom)
import Data.Scientific
import Data.Vector (Vector)
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Store.Encode
import Database.PostgreSQL.Protocol.Store.Decode
import Database.PostgreSQL.Protocol.DataRows
import qualified Database.PostgreSQL.Protocol.Codecs.Decoders as PD
import qualified Database.PostgreSQL.Protocol.Codecs.Encoders as PE
import qualified Database.PostgreSQL.Protocol.Codecs.PgTypes  as PGT

main :: IO ()
main = defaultMain
    [ bgroup "Encoding"
        [ bench "Message" $ nf encodeMessage queryParams
        , bench "Scientific" $ nf (runEncode . PE.numeric) testScientific
        , bench "UTCTime" $ nf (runEncode . PE.timestamptz) testUTCTime
        , bench "UUID" $ nf (runEncode . PE.uuid) testUUID
        ]
    , bgroup "Decoding"
        [ bench "Message" $ nf decodeMessage testDataRows
        , bench "Message as bytes" $ nf decodeMessageBytes testDataRows
        , bench "Scientific" $ nf (runDecode $ PD.numeric 0) testScientificEncoded
        , bench "UTCTime" $ nf (runDecode $ PD.timestamptz 0) testUTCTimeEncoded
        , bench "UUID" $ nf (runDecode $ PD.uuid 0) testUUIDEncoded
        ]
    ]

type QueryParams 
    = (Bool, ByteString, Double, DiffTime, Scientific, UTCTime, UUID)

{-# NOINLINE queryParams #-}
queryParams :: QueryParams
queryParams =
    ( True
    , "aaaaaaaaaaaa"
    , 3.1415926
    , fromIntegral 20000000
    , scientific 1111111111111 (-18)
    , unsafePerformIO getCurrentTime
    , unsafePerformIO nextRandom
    )

testScientific :: Scientific
testScientific = scientific 11111111111111 (-18)

testScientificEncoded :: ByteString
testScientificEncoded = runEncode $ PE.numeric testScientific

{-# NOINLINE testUTCTime #-}
testUTCTime :: UTCTime
testUTCTime = unsafePerformIO getCurrentTime

testUTCTimeEncoded :: ByteString
testUTCTimeEncoded = runEncode $ PE.timestamptz testUTCTime

{-# NOINLINE testUUID #-}
testUUID :: UUID
testUUID = unsafePerformIO nextRandom

testUUIDEncoded :: ByteString
testUUIDEncoded = runEncode $ PE.uuid testUUID

encodeMessage :: QueryParams -> ByteString
encodeMessage params = runEncode $ 
    encodeClientMessage parseMessage <> encodeClientMessage bindMessage 
  where
    bindMessage = Bind (PortalName "") stmtName Binary 
        (encodedParams params) Binary
    encodedParams (a, b, c, d, e, f, g) = V.fromList
        [ Just . runEncode $ PE.bool a
        , Just . runEncode $ PE.bytea b
        , Just . runEncode $ PE.float8 c
        , Just . runEncode $ PE.interval d
        , Just . runEncode $ PE.numeric e
        , Just . runEncode $ PE.timestamptz f
        , Just . runEncode $ PE.uuid g
        ]
    parseMessage = Parse stmtName stmt oids
    stmtName = StatementName "_pw_statement_0010"
    stmt = StatementSQL 
        "SELECT a, b, c FROM table_name WHERE name LIKE $1 AND a > $2"
    oids = V.fromList $ map PGT.oidType
        [ PGT.bool
        , PGT.bytea
        , PGT.float8
        , PGT.interval
        , PGT.numeric
        , PGT.timestamptz
        , PGT.uuid
        ]

decodeMessage :: DataRows -> Vector QueryParams
decodeMessage = decodeManyRows (PD.dataRowHeader *> decoder)
  where 
    decoder = (,,,,,,) 
        <$> PD.getNonNullable PD.bool
        <*> PD.getNonNullable PD.bytea
        <*> PD.getNonNullable PD.float8
        <*> PD.getNonNullable PD.interval 
        <*> PD.getNonNullable PD.numeric
        <*> PD.getNonNullable PD.timestamptz 
        <*> PD.getNonNullable PD.uuid

decodeMessageBytes 
    :: DataRows 
    -> Vector ( ByteString, ByteString, ByteString, ByteString, ByteString
              , ByteString, ByteString )
decodeMessageBytes = decodeManyRows (PD.dataRowHeader *> decoder)
  where 
    decoder = (,,,,,,) 
        <$> PD.getNonNullable PD.bytea
        <*> PD.getNonNullable PD.bytea
        <*> PD.getNonNullable PD.bytea
        <*> PD.getNonNullable PD.bytea 
        <*> PD.getNonNullable PD.bytea
        <*> PD.getNonNullable PD.bytea 
        <*> PD.getNonNullable PD.bytea

{-# NOINLINE testDataRows #-}
testDataRows :: DataRows
testDataRows = DataRows chunk (DataRows chunk (DataRows chunk Empty))
  where
    row = unsafePerformIO $ B.readFile "bench/row.out"
    rows = fold $ replicate 10 row
    chunk = DataChunk 10 rows
