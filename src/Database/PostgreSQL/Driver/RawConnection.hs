{-# language FlexibleContexts #-}
module Database.PostgreSQL.Driver.RawConnection where

import Control.Monad (void)
import Control.Exception (bracketOnError)
import Safe (headMay)
import Data.Monoid ((<>))
import System.Socket (socket, AddressInfo(..), getAddressInfo, socketAddress,
                      aiV4Mapped, Socket, connect, close, receive, send)
import System.Socket.Family.Inet (Inet)
import System.Socket.Type.Stream (Stream)
import System.Socket.Protocol.TCP (TCP)
import System.Socket.Family.Unix (Unix, socketAddressUnixPath)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS(pack)

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.Settings

-- | Abstraction over raw socket connection or tls connection.
data RawConnection = RawConnection
    { rFlush   :: IO ()
    , rClose   :: IO ()
    , rSend    :: B.ByteString -> IO ()
    , rReceive :: Int -> IO B.ByteString
    }

defaultUnixPathDirectory :: B.ByteString
defaultUnixPathDirectory = "/var/run/postgresql"

unixPathFilename :: B.ByteString
unixPathFilename = ".s.PGSQL."

-- | Creates a raw connection and connects to a server.
-- Throws `SocketException`, `AddressException`.
createRawConnection :: ConnectionSettings -> IO (Either Error RawConnection)
createRawConnection settings
        | host == ""              = unixConnection defaultUnixPathDirectory
        | "/" `B.isPrefixOf` host = unixConnection host
        | otherwise               = tcpConnection
  where
    createAndConnect Nothing creating = throwAuthErrorInIO AuthInvalidAddress
    createAndConnect (Just address) creating =
        bracketOnError creating close $ \s -> do
            connect s address
            pure . Right $ constructRawConnection s

    unixConnection dirPath = do
        let mAddress = socketAddressUnixPath $ makeUnixPath dirPath
        createAndConnect mAddress (socket :: IO (Socket Unix Stream Unix))

    tcpConnection = do
        mAddress <- fmap socketAddress . headMay <$>
            (getAddressInfo (Just host) (Just portStr) aiV4Mapped
             :: IO [AddressInfo Inet Stream TCP])
        createAndConnect mAddress (socket :: IO (Socket Inet Stream TCP))

    portStr = BS.pack . show $ settingsPort settings
    host    = settingsHost settings
    makeUnixPath dirPath =
        -- 47 - `/`, removing slash on the end of the path
        let dir = B.reverse . B.dropWhile (== 47) $ B.reverse dirPath
        in dir <> "/" <> unixPathFilename <> portStr

constructRawConnection :: Socket f t p -> RawConnection
constructRawConnection s = RawConnection
    { rFlush = pure ()
    , rClose = close s
    , rSend = \msg -> void $ send s msg mempty
    , rReceive = \n -> receive s n mempty
    }

