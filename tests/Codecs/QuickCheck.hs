module Codecs.QuickCheck where

import Test.Tasty
import Test.QuickCheck
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Monadic

import qualified Data.ByteString as B
import qualified Data.Vector as V

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
    :: (Eq a, Arbitrary a )
    => Connection 
    -> Oid -> (a -> Encode) -> PD.FieldDecoder a 
    -> a -> Property
makeCodecProperty c oid encoder fd v = monadicIO $ do
    let bs = runEncode $ encoder v
        q = Query "SELECT $1" (V.fromList [(oid, Just bs)])
                    Binary Binary AlwaysCache
        decoder = PD.dataRowHeader *> PD.getNonNullable fd
    r <- run $ do
        sendBatchAndSync c [q]
        dr <- readNextData c
        waitReadyForQuery c
        either (error . show) (pure . decodeOneRow decoder) dr

    assert $ v == r

-- | Makes Tasty test tree.
mkCodecTest 
    :: (Eq a, Arbitrary a, Show a) 
    => TestName -> PGT.Oids -> (a -> Encode) -> PD.FieldDecoder a
    -> TestTree
mkCodecTest name oids encoder decoder = testPropertyConn name $ \c ->
    makeCodecProperty c (PGT.oidType oids) encoder decoder

testCodecsEncodeDecode :: TestTree
testCodecsEncodeDecode = testGroup "Codecs property 'encode . decode = id'"
    [ mkCodecTest "bool" PGT.bool PE.bool PD.bool
    , mkCodecTest "bytea" PGT.bytea PE.bytea PD.bytea
    , mkCodecTest "char" PGT.char PE.char PD.char
    -- TODO instance
    -- , mkCodecTest "date" PGT.date PE.date PD.date
    , mkCodecTest "float4" PGT.float4 PE.float4 PD.float4
    , mkCodecTest "float8" PGT.float8 PE.float8 PD.float8
    , mkCodecTest "int2" PGT.int2 PE.int2 PD.int2
    , mkCodecTest "int4" PGT.int4 PE.int4 PD.int4
    , mkCodecTest "int8" PGT.int8 PE.int8 PD.int8
    -- TODO intstance
    -- , mkCodecTest "interval" PGT.interval PE.interval PD.interval
    , mkCodecTest "json" PGT.json PE.bsJsonText PD.bsJsonText
    , mkCodecTest "jsonb" PGT.jsonb PE.bsJsonBytes PD.bsJsonBytes
    -- TODO
    -- , mkCodecTest "numeric" PGT.numeric PE.numeric PD.numeric
    , mkCodecTest "text" PGT.text PE.bsText PD.bsText
    -- TODO make instance
    -- , mkCodecTest "timestamp" PGT.timestamp PE.timestamp PD.timestamp
    -- TODO make instance
    -- , mkCodecTest "timestamptz" PGT.timestamptz PE.timestamptz PD.timestamptz
    -- TODO make instance
    -- , mkCodecTest "uuid" PGT.uuid PE.uuid PD.uuid
    ]

-- TODO right instance
instance Arbitrary B.ByteString where
    arbitrary = oneof [pure "1", pure "2"]
