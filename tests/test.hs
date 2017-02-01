import Test.Tasty (defaultMain, testGroup)

import Protocol
import Driver
import Misc

main :: IO ()
main = defaultMain $ testGroup "Postgres-wire"
    [ testProtocolMessages
    , testDriver
    , testMisc
    ]

