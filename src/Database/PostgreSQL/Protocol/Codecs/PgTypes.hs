{-
   Oids for built-in types.
-}
module Database.PostgreSQL.Protocol.Codecs.PgTypes where

import Data.Int (Int32)

import Database.PostgreSQL.Protocol.Types

data Oids = Oids
    { oidType      :: {-# UNPACK #-} !Oid
    , oidArrayType :: {-# UNPACK #-} !Oid
    } deriving (Show, Eq)

mkOids :: Int32 -> Int32 -> Oids
mkOids a b = Oids (Oid a) (Oid b)

--
-- Primitives
--

bool :: Oids
bool = mkOids 16 1000

bytea :: Oids
bytea = mkOids 17 1001

char :: Oids         
char = mkOids 18 1002

date :: Oids        
date = mkOids 1082 1182

float4 :: Oids        
float4 = mkOids 700 1021

float8 :: Oids         
float8 = mkOids 701 1022

int2 :: Oids           
int2 = mkOids 21 1005 

int4 :: Oids         
int4 = mkOids 23 1007

int8 :: Oids        
int8 = mkOids 20 1016

interval :: Oids    
interval = mkOids 1186 1187 

json :: Oids               
json = mkOids 114 199     

jsonb :: Oids
jsonb = mkOids 3802 3807

numeric :: Oids          
numeric = mkOids 1700 1231 

text :: Oids              
text = mkOids 25 1009    

timestamp :: Oids       
timestamp = mkOids 1114 1115 

timestamptz :: Oids         
timestamptz = mkOids 1184 1185

uuid :: Oids                 
uuid = mkOids 2950 2951

--
-- Ranges
--

daterange :: Oids
daterange = mkOids 3912 3913

int4range :: Oids
int4range = mkOids 3904 3905

int8range :: Oids
int8range = mkOids 3926 3927

numrange :: Oids
numrange = mkOids 3906 3907

tsrange :: Oids
tsrange = mkOids 3908 3909

tstzrange :: Oids
tstzrange = mkOids 3910 3911
