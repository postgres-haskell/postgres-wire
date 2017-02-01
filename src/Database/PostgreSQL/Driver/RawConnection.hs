{-# language FlexibleContexts #-}
module Database.PostgreSQL.Driver.RawConnection where

import Control.Monad (void)
import Safe (headMay)
import Data.Monoid ((<>))
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BS(pack)
import System.Socket hiding (Error)
import System.Socket.Family.Inet
import System.Socket.Type.Stream
import System.Socket.Protocol.TCP
import System.Socket.Family.Unix

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
createRawConnection :: ConnectionSettings -> IO (Either Error RawConnection)
createRawConnection settings
        | host == ""              = unixConnection defaultUnixPathDirectory
        | "/" `B.isPrefixOf` host = unixConnection host
        | otherwise               = tcpConnection
  where
    unixConnection dirPath = do
        let mAddress = socketAddressUnixPath $ makeUnixPath dirPath
        createAndConnect mAddress (socket :: IO (Socket Unix Stream Unix))

    tcpConnection = do
        mAddress <- fmap socketAddress . headMay <$>
            (getAddressInfo (Just host) (Just portStr) aiV4Mapped
             :: IO [AddressInfo Inet Stream TCP])
        createAndConnect mAddress (socket :: IO (Socket Inet Stream TCP))

    createAndConnect Nothing creating = throwAuthErrorInIO AuthInvalidAddress
    createAndConnect (Just address) creating = do
        s <- creating
        connect s address
        pure . Right $ constructRawConnection s

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

