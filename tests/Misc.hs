module Misc where

import qualified Data.ByteString as B
import Data.Foldable
import Test.Tasty
import Test.Tasty.HUnit

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Protocol.Decoders


testMisc :: TestTree
testMisc = testGroup "Misc"
    [ testCase "Parser server version" testParseServerVersion
    ]

testParseServerVersion :: IO ()
testParseServerVersion = traverse_ testSingle
    [ ("9.2", ServerVersion 9 2 0 "")
    , ("9.2.1", ServerVersion 9 2 1 "")
    , ("9.4beta1", ServerVersion 9 4 0 "beta1")
    , ("10devel", ServerVersion 10 0 0 "devel")
    , ("10beta2", ServerVersion 10 0 0 "beta2")
    ]
  where
    testSingle (str, result) = case parseServerVersion str of
        Left _ -> assertFailure "Should be Right, got error "
        Right v -> result @=? v

