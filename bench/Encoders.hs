module Encoders where

import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString (ByteString)
import Data.Vector as V(fromList, empty)
import Criterion.Main

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Store.Encode

-- main = defaultMain
--     [ bgroup "Main bench"
--         [
--         ]
--     ]

benchEncode =
    [ bgroup "Protocol encoding"
        [ benchMessage "Flush" Flush
        , benchMessage "Execute" $ Execute (PortalName "") noLimitToReceive
        , benchParse0
        , benchParse3
        , benchParse5
        , benchParse10
        , benchBind0
        , benchBind3
        , benchBind5
        , benchBind10
        ]
    ]

benchMessage :: String -> ClientMessage -> Benchmark
benchMessage name = bench name . nf encodeMessage
  where
    encodeMessage = runEncode . encodeClientMessage

parseMessage :: Int -> ClientMessage
parseMessage n = Parse (StatementName "53") (StatementSQL
    "SELECT type, name, a, b, c FROM table_name WHERE name LIKE $1 AND a > $2")
    (V.fromList $ replicate n (Oid 23))

bindMessage :: Int -> ClientMessage
bindMessage n = Bind (PortalName "") (StatementName "31") Binary
    (V.fromList $ replicate n (Just "aaaaaaaaaaaaaaaaaaa")) Binary

benchParse0, benchParse3, benchParse5, benchParse10 :: Benchmark
benchParse0 = benchMessage "Parse 0" $ parseMessage 0
benchParse3 = benchMessage "Parse 3" $ parseMessage 3
benchParse5 = benchMessage "Parse 5" $ parseMessage 5
benchParse10 = benchMessage "Parse 10" $ parseMessage 10

benchBind0, benchBind3, benchBind5, benchBind10 :: Benchmark
benchBind0 = benchMessage "Bind 0" $ bindMessage 0
benchBind3 = benchMessage "Bind 3" $ bindMessage 3
benchBind5 = benchMessage "Bind 5" $ bindMessage 5
benchBind10 = benchMessage "Bind 10" $ bindMessage 10

