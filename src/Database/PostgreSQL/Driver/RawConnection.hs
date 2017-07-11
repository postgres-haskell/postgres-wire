{-# language FlexibleContexts #-}
module Database.PostgreSQL.Driver.RawConnection 
    ( RawConnection(..)
    , createRawConnection
    ) where

import Control.Monad (void, when)
import Control.Exception (bracketOnError, try)
import Data.Monoid ((<>))
import Foreign (castPtr, plusPtr)
import System.Socket (socket, AddressInfo(..), getAddressInfo, socketAddress,
                      aiV4Mapped, AddressInfoException, Socket, connect,
                      close, receive, send)
import System.Socket.Unsafe (unsafeReceive)
import System.Socket.Family.Inet (Inet)
import System.Socket.Type.Stream (Stream, sendAll)
import System.Socket.Protocol.TCP (TCP)
import System.Socket.Protocol.Default (Default)
import System.Socket.Family.Unix (Unix, socketAddressUnixPath)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Char8 as BS(pack)

import Database.PostgreSQL.Driver.Error
import Database.PostgreSQL.Driver.Settings

-- | Abstraction over raw socket connection or tls connection.
data RawConnection = RawConnection
    { rFlush   :: IO ()
    , rClose   :: IO ()
    , rSend    :: B.ByteString -> IO ()
    -- ByteString that should be prepended to received ByteString
    , rReceive :: B.ByteString -> Int -> IO B.ByteString
    }

defaultUnixPathDirectory :: B.ByteString
defaultUnixPathDirectory = "/var/run/postgresql"

unixPathFilename :: B.ByteString
unixPathFilename = ".s.PGSQL."

-- | Creates a raw connection and connects to a server.
-- Throws `SocketException`.
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
        createAndConnect mAddress (socket :: IO (Socket Unix Stream Default))

    tcpConnection = fmap excToError . try $ do
        mAddress <- fmap socketAddress . headMay <$>
            (getAddressInfo (Just host) (Just portStr) aiV4Mapped
             :: IO [AddressInfo Inet Stream TCP])
        createAndConnect mAddress (socket :: IO (Socket Inet Stream TCP))

    excToError (Left ex) = Left . AuthError $ AuthAddressException ex
    excToError (Right v) = v

    portStr = BS.pack . show $ settingsPort settings
    host    = settingsHost settings
    makeUnixPath dirPath =
        -- 47 - `/`, removing slash on the end of the path
        let dir = B.reverse . B.dropWhile (== 47) $ B.reverse dirPath
        in dir <> "/" <> unixPathFilename <> portStr

    headMay []    = Nothing
    headMay (x:_) = Just x

constructRawConnection :: Socket f Stream p -> RawConnection
constructRawConnection s = RawConnection
    { rFlush = pure ()
    , rClose = close s
    , rSend = \msg -> void $ sendAll s msg mempty
    , rReceive = rawReceive s
    }

{-# INLINE rawReceive #-}
rawReceive :: Socket f Stream p -> B.ByteString -> Int -> IO B.ByteString
rawReceive s bs n = B.unsafeUseAsCStringLen bs $ \(prevPtr, prevLen) ->
    let bufSize = prevLen + n
    in B.createUptoN bufSize $ \bufPtr -> do
        B.memcpy bufPtr (castPtr prevPtr) prevLen
        len <- unsafeReceive s (bufPtr `plusPtr` prevLen) 
                (fromIntegral n) mempty
        -- Received empty string means closed connection by the remote host
        when (len == 0) throwClosedException
        pure $ prevLen + fromIntegral len
