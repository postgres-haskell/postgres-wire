import Test.Tasty (defaultMain, testGroup)

import Protocol

main :: IO ()
main = defaultMain $ testGroup "Postgres-wire"
    [ testProtocolMessages
    ]

