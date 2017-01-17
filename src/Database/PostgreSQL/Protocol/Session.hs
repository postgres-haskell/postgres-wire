data Count = One | Many
    deriving (Eq, Show)

data Session a
    = Done a
    | forall r . Decode r => Receive (r -> Session a)
    | Send Count [Request] (Session a)

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

runSession :: Show a => Connection -> Session a -> IO a
runSession conn@(Connection sock _ chan) = go
  where
    go (Done x) = do
        putStrLn $ "Return " ++ show x
        pure x
    go (Receive f) = do
        putStrLn "Receiving"
        -- TODO receive here
        -- x <- receive
        x <- getLine
        go (f $ decode x)
    go (Send _ rs c) = do
        putStrLn "Sending requests "
        -- TODO send requests here in batch
        sendBatch conn rs
        go c

