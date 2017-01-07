module Database.PostgreSQL.Protocol.Encoders where

import Data.Word
import Data.Int
import Data.Monoid
import Data.ByteString.Lazy as BL
import Data.ByteString.Builder
import qualified Data.ByteString as B

import Database.PostgreSQL.Protocol.Types
-- | Protocol Version 3.0, major version in the first word16
currentVersion :: Int32
currentVersion = 3 * 256 * 256

encodeStartMessage :: StartMessage -> Builder
-- Options except user and database are not supported
encodeStartMessage (StartupMessage uname dbname) =
    int32BE (len + 4) <> payload
  where
    len     = fromIntegral $ BL.length $ toLazyByteString payload
    payload = int32BE currentVersion <>
              pgString "user" <> pgString uname <>
              pgString "database" <> pgString dbname <> word8 0
encodeStartMessage SSLRequest = undefined

encodeClientMessage :: ClientMessage -> Builder
encodeClientMessage = undefined


----------
-- Utils
---------

-- | C-like string
pgString :: B.ByteString -> Builder
pgString s = byteString s <> word8 0

prependHeader :: Builder -> Char -> Builder
prependHeader builder c =
    let payload = toLazyByteString builder
        len = fromIntegral $ BL.length payload
    in char8 c <> int32BE len <> lazyByteString payload

