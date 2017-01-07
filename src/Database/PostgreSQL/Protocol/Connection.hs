module Database.PostgreSQL.Protocol.Connection where


import qualified Data.ByteString as B
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Control.Monad
import Data.Monoid
import Control.Concurrent
import Data.Maybe (fromJust)
import System.Socket hiding (connect, close)
import qualified System.Socket as Socket (connect, close)
import System.Socket.Family.Inet6
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP
import System.Socket.Family.Unix

import Database.PostgreSQL.Protocol.Settings
import Database.PostgreSQL.Protocol.Encoders
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
    tid <- forkIO $ forever $ do
        r <- receive s 4096 mempty
        print r
    sendMessage s $ encodeStartMessage $ consStartupMessage settings
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

