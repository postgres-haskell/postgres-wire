{-# language ApplicativeDo #-}
{-# language OverloadedLists #-}
{-# language OverloadedStrings #-}
{-# language ExistentialQuantification #-}
{-# language TypeSynonymInstances #-}
{-# language FlexibleInstances #-}
module Database.PostgreSQL.Session where

import Control.Monad
import Control.Applicative
import Data.Monoid
import Data.Int
import Data.ByteString (ByteString)
import Data.Either
import qualified Data.Vector as V

import           PostgreSQL.Binary.Encoder (int8_int64, run)
import qualified PostgreSQL.Binary.Decoder as D(int, run)

import Database.PostgreSQL.Protocol.Types
import Database.PostgreSQL.Connection
import Database.PostgreSQL.Settings

data Count = One | Many
    deriving (Eq, Show)

data Session a
    = Done a
    | forall r . Decode r => Receive (r -> Session a)
    | Send Count [Query] (Session a)

instance Functor Session where
    f `fmap` (Done a) = Done $ f a
    f `fmap` (Receive g) = Receive $ fmap f . g
    f `fmap` (Send n br c) = Send n br (f <$> c)

instance Applicative Session where
    pure = Done

    f <*> x = case (f, x) of
        (Done g, Done y) -> Done (g y)
        (Done g, Receive next) -> Receive $ fmap g . next
        (Done g, Send n br c) -> Send n br (g <$> c)

        (Send n br c, Done y) -> Send n br (c <*> pure y)
        (Send n br c, Receive next)
            -> Send n br $ c <*> Receive next
        (Send n1 br1 c1, Send n2 br2 c2)
            -> if n1 == One
               then Send n2 (br1 <> br2) (c1 <*> c2)
               else Send n1 br1 (c1 <*> Send n2 br2 c2)

        (Receive next1, Receive next2) ->
            Receive  $ (\g -> Receive $ (g <*> ) . next2) . next1
        (Receive next, Done y) -> Receive $ (<*> Done y) . next
        (Receive next, Send n br c)
            -> Receive $ (<*> Send n br c) . next

instance Monad Session where
    return = pure

    m >>= f = case m of
        Done a -> f a
        Receive g -> Receive $ (>>=f) . g
        Send _n br c -> Send Many br (c >>= f)

    (>>) = (*>)

class Encode a where
    encode :: a -> ByteString
    getOid :: a -> Oid

class Decode a where
    decode :: ByteString -> a

instance Encode Int64 where
    encode   = run int8_int64
    getOid _ = Oid 20

instance Decode Int64 where
    decode = fromRight . D.run D.int
      where
        fromRight (Right v) = v
        fromRight _ = error "bad fromRight"

data SessionQuery a b = SessionQuery { sqStatement :: ByteString }
    deriving (Show)

query :: (Encode a, Decode b) => SessionQuery a b -> a -> Session b
query sq val =
    let q = Query { qStatement = sqStatement sq
                  , qOids = [getOid val]
                  , qValues = [encode val]
                  , qParamsFormat = Binary
                  , qResultFormat = Binary }
    in Send One [q] $ Receive Done

runSession :: Show a => Connection -> Session a -> IO (Either Error a)
runSession conn = go 0
  where
    go n (Done x) = do
        putStrLn $ "Return " ++ show x
        when (n > 0) $ void $ sendSync conn >> readReadyForQuery conn
        pure $ Right x
    go n (Receive f) = do
        putStrLn "Receiving"
        r <- readNextData conn
        case r of
            Left e -> pure $ Left e
            Right (DataMessage rows) -> go n (f $ decode $ V.head $ head rows)
    go n (Send _ qs c) = do
        putStrLn "Sending requests "
        sendBatch conn qs
        sendFlush conn
        go (n + 1) c

q1 :: SessionQuery Int64 Int64
q1 = SessionQuery "SELECT $1"

q2 :: SessionQuery Int64 Int64
q2 = SessionQuery "SELECT count(*) from a where v < $1"

q3 :: SessionQuery Int64 Int64
q3 = SessionQuery "SELECT 5 + $1"

testSession :: IO ()
testSession  = do
    c <- connect defaultConnectionSettings
    r <- runSession c $ do
        b <- query q1 10
        a <- query q2 b
        query q3 a
    print r
    close c

