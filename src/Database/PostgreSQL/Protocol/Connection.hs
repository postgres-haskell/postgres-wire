{-# language OverloadedLists #-}
{-# language OverloadedStrings #-}
module Database.PostgreSQL.Protocol.Connection where


import qualified Data.ByteString as B
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Control.Monad
import Data.Traversable
import Data.Foldable
import Control.Applicative
import Data.Monoid
import Control.Concurrent
import Data.Binary.Get (Decoder(..), runGetIncremental, pushChunk)
import Data.Maybe (fromJust)
import System.Socket hiding (connect, close)
import qualified System.Socket as Socket (connect, close)
import System.Socket.Family.Inet6
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP
import System.Socket.Family.Unix

import Database.PostgreSQL.Protocol.Settings
import Database.PostgreSQL.Protocol.Encoders
import Database.PostgreSQL.Protocol.Decoders
import Database.PostgreSQL.Protocol.Types


type UnixSocket = Socket Unix Stream Unix
-- data Connection = Connection (Socket Inet6 Stream TCP)
data Connection = Connection UnixSocket ThreadId

address :: SocketAddress Unix
address = fromJust $ socketAddressUnixPath "/var/run/postgresql/.s.PGSQL.5432"

connect :: ConnectionSettings -> IO Connection
connect settings = do
    s <- socket
    Socket.connect s address
    sendMessage s $ encodeStartMessage $ consStartupMessage settings
    r <- receive s 4096 mempty
    readAuthMessage r

    tid <- forkIO $ receiverThread s
    pure $ Connection s tid

close :: Connection -> IO ()
close (Connection s tid) = do
    killThread tid
    Socket.close s

consStartupMessage :: ConnectionSettings -> StartMessage
consStartupMessage stg = StartupMessage (connUser stg) (connDatabase stg)

sendMessage :: UnixSocket -> Builder -> IO ()
sendMessage sock msg = void $ do
    let smsg = toStrict $ toLazyByteString msg
    putStrLn "sending message:"
    print smsg
    send sock smsg mempty

readAuthMessage :: B.ByteString -> IO ()
readAuthMessage s =
    case pushChunk (runGetIncremental decodeAuthResponse) s of
        Done _ _ r -> case r of
            AuthenticationOk -> putStrLn "Auth ok"
            _                -> error "Invalid auth"
        f -> error $ show s

receiverThread :: UnixSocket -> IO ()
receiverThread sock = forever $ do
    r <- receive sock 4096 mempty
    print r
    go r
  where
    decoder = runGetIncremental decodeServerMessage
    go str = case pushChunk decoder str of
        Done rest _ v -> do
            print v
            unless (B.null rest) $ go rest
        Partial _ -> error "Partial"
        Fail _ _ e -> error e

sendQuery :: Connection -> IO ()
sendQuery (Connection s _) = do
    sendMessage s $ encodeClientMessage $ Parse "test" "SELECT $1 + $2" [23, 23]
    sendMessage s $ encodeClientMessage $
        Bind "test" "test" Text ["2", "3"] Text
    sendMessage s $ encodeClientMessage $ Execute "test"
    sendMessage s $ encodeClientMessage Sync

test :: IO ()
test = do
    c <- connect defaultConnectionSettings
    sendQuery c
    threadDelay 3000
    close c

