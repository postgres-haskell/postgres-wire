import Test.Tasty (defaultMain, testGroup)

import Protocol
import Driver

main :: IO ()
main = defaultMain $ testGroup "Postgres-wire"
    [ testProtocolMessages
    , testDriver
    ]

