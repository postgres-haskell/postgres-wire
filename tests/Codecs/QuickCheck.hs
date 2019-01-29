module Codecs.QuickCheck where

import Data.Monoid ((<>))
import Test.Tasty
import Test.QuickCheck
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Monadic

import Data.Scientific as S
import Data.Time
import Text.Printf
import Data.List as L
import Data.UUID (UUID, fromWords)
import Data.String (IsString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC

import Database.PostgreSQL.Driver
import Database.PostgreSQL.Protocol.DataRows
import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Store.Encode
import Database.PostgreSQL.Protocol.Store.Decode
import qualified Database.PostgreSQL.Protocol.Codecs.Decoders as PD
import qualified Database.PostgreSQL.Protocol.Codecs.Encoders as PE
import qualified Database.PostgreSQL.Protocol.Codecs.PgTypes  as PGT
import Connection
import Codecs.Runner


-- | Makes property that if here is a value then encoding and sending it 
--  to PostgreSQL, and receiving back returns the same value.
makeCodecProperty 
    :: (Show a, Eq a, Arbitrary a)
    => Connection 
    -> Oid -> (a -> Encode) -> PD.FieldDecoder a 
    -> a -> Property
makeCodecProperty c oid encoder fd v = monadicIO $ do
    let q = Query "SELECT $1" [(oid, Just $ encoder v)]
                    Binary Binary AlwaysCache
        decoder = PD.dataRowHeader *> PD.getNonNullable fd
    r <- run $ do
        sendBatchAndSync c [q]
        dr <- readNextData c
        waitReadyForQuery c
        either (error . show) (pure . decodeOneRow decoder) dr

    assertQCEqual v r

-- | Makes a property that encoded value is correctly parsed and printed
-- by PostgreSQL.
makeCodecEncodeProperty
    :: Arbitrary a
    => Connection
    -> Oid
    -> B.ByteString
    -> (a -> Encode)
    -> (a -> String)
    -> a -> Property
makeCodecEncodeProperty c oid queryString encoder fPrint v = monadicIO $ do
    let q = Query queryString [(oid, Just $ encoder v)]
                Binary Text AlwaysCache
        decoder = PD.dataRowHeader *> PD.getNonNullable PD.bytea
    r <- run $ do
        sendBatchAndSync c [q]
        dr <- readNextData c
        waitReadyForQuery c
        either (error . show) (pure . BC.unpack . decodeOneRow decoder) dr

    assertQCEqual (fPrint v) r

assertQCEqual :: (Eq a, Show a, Monad m) => a -> a -> PropertyM m ()
assertQCEqual a b 
    | a == b    = pure ()
    | otherwise = fail $ 
        "Equal assertion failed. Expected:\n" <> show a
        <> "\nbut got:\n" <> show b

-- | Makes Tasty test tree.
mkCodecTest 
    :: (Eq a, Arbitrary a, Show a) 
    => TestName -> PGT.Oids -> (a -> Encode) -> PD.FieldDecoder a
    -> TestTree
mkCodecTest name oids encoder decoder = testPropertyConn name $ \c ->
    makeCodecProperty c (PGT.oidType oids) encoder decoder

mkCodecEncodeTest
    :: (Arbitrary a, Show a)
    => TestName -> PGT.Oids -> B.ByteString -> (a -> Encode) -> (a -> String)
    -> TestTree
mkCodecEncodeTest name oids queryString encoder fPrint = 
    testPropertyConn name $ \c ->
        makeCodecEncodeProperty c (PGT.oidType oids) queryString encoder fPrint

testCodecsEncodeDecode :: TestTree
testCodecsEncodeDecode = testGroup "Codecs property 'encode . decode = id'"
    [ mkCodecTest "bool" PGT.bool PE.bool PD.bool
    , mkCodecTest "bytea" PGT.bytea PE.bytea PD.bytea
    , mkCodecTest "char" PGT.char (PE.char . unAsciiChar)
                                  (fmap AsciiChar <$> PD.char)
    , mkCodecTest "date" PGT.date PE.date PD.date
    , mkCodecTest "float4" PGT.float4 PE.float4 PD.float4
    , mkCodecTest "float8" PGT.float8 PE.float8 PD.float8
    , mkCodecTest "int2" PGT.int2 PE.int2 PD.int2
    , mkCodecTest "int4" PGT.int4 PE.int4 PD.int4
    , mkCodecTest "int8" PGT.int8 PE.int8 PD.int8
    , mkCodecTest "interval" PGT.interval PE.interval PD.interval
    , mkCodecTest "json" PGT.json (PE.bsJsonText . unJsonString)
                                  (fmap JsonString <$> PD.bsJsonText)
    , mkCodecTest "jsonb" PGT.jsonb (PE.bsJsonBytes . unJsonString)
                                    (fmap JsonString <$> PD.bsJsonBytes)
    , mkCodecTest "numeric" PGT.numeric PE.numeric PD.numeric
    , mkCodecTest "text" PGT.text PE.bsText PD.bsText
    , mkCodecTest "time" PGT.time PE.time PD.time
    , mkCodecTest "timetz" PGT.timetz PE.timetz PD.timetz
    , mkCodecTest "timestamp" PGT.timestamp PE.timestamp PD.timestamp
    , mkCodecTest "timestamptz" PGT.timestamptz PE.timestamptz PD.timestamptz
    , mkCodecTest "uuid" PGT.uuid PE.uuid PD.uuid
    ]

testCodecsEncodePrint :: TestTree
testCodecsEncodePrint = testGroup 
    "Codecs property 'Encoded value Postgres = value in Haskell'"
    [ mkCodecEncodeTest "bool" PGT.bool qBasic PE.bool displayBool
    , mkCodecEncodeTest "date" PGT.date qBasic PE.date show
    , mkCodecEncodeTest "float8" PGT.float8 
        "SELECT trim(to_char($1, '99999999999990.9999999999'))" 
         PE.float8 (printf "%.10f")
    , mkCodecEncodeTest "int8" PGT.int8 qBasic PE.int8 show
    , mkCodecEncodeTest "interval" PGT.interval 
        "SELECT extract(epoch from $1)||'s'" PE.interval show
    , mkCodecEncodeTest "numeric" PGT.numeric qBasic PE.numeric 
        displayScientific
    , mkCodecEncodeTest "timestamp" PGT.timestamp qBasic PE.timestamp show
    , mkCodecEncodeTest "timestamptz" PGT.timestamptz 
        "SELECT ($1 at time zone 'UTC')||' UTC'" PE.timestamptz show
    , mkCodecEncodeTest "uuid" PGT.uuid qBasic PE.uuid show
    ]
  where
    qBasic = "SELECT $1"

    displayScientific s | isInteger s = show $ ceiling s
                        | otherwise = formatScientific S.Fixed Nothing s

    displayBool False = "f"
    displayBool True  = "t"

--
-- Orphan instances
--

newtype AsciiChar = AsciiChar { unAsciiChar :: Char }
    deriving (Show, Eq)

instance Arbitrary AsciiChar where
    arbitrary = AsciiChar <$> choose ('\0', '\127')

-- Helper to generate valid json strings
newtype JsonString = JsonString { unJsonString :: B.ByteString }
    deriving (Show, Eq, IsString)

instance Arbitrary JsonString where
    arbitrary = oneof $ map pure 
        [ "{}"
        , "{\"a\": 5}"
        , "{\"b\": [1, 2, 3]}"
        ]

instance Arbitrary B.ByteString where
    arbitrary = do
        len <- choose (0, 1024) 
        B.pack <$> vectorOf len (choose (1, 127))

instance Arbitrary Day where
    arbitrary = ModifiedJulianDay <$> choose (-100000, 100000)

instance Arbitrary DiffTime where
    arbitrary = secondsToDiffTime <$> choose (0, 86400 - 1)

instance Arbitrary TimeOfDay where
    arbitrary = timeToTimeOfDay <$> arbitrary

instance Arbitrary LocalTime where
    arbitrary = LocalTime <$> arbitrary <*> fmap timeToTimeOfDay arbitrary

instance Arbitrary UTCTime where
    arbitrary = UTCTime <$> arbitrary <*> arbitrary

instance Arbitrary UUID where
    arbitrary = fromWords <$> arbitrary <*> arbitrary 
                          <*> arbitrary <*> arbitrary

instance Arbitrary Scientific where
    arbitrary = do
        c <- choose (-100000000, 100000000) 
        e <- choose (-10, 10)
        pure . normalize $ scientific c e
