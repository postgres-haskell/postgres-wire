module Codecs.Runner where

import Data.Typeable
import Data.Tagged
import Test.Tasty.QuickCheck
import Test.Tasty.Providers
import Test.Tasty.Options
import qualified Test.QuickCheck as QC

import Database.PostgreSQL.Driver
import Connection

newtype ConnQC = ConnQC (Connection -> QC.Property)
  deriving Typeable

-- | Create a 'Test' for a QuickCheck 'QC.Testable' property
testPropertyConn :: QC.Testable a => TestName -> (Connection -> a) -> TestTree
testPropertyConn name fprop = singleTest name . ConnQC $ QC.property . fprop 

instance IsTest ConnQC where
  testOptions = retag (testOptions :: Tagged QC [OptionDescription])

  run opts (ConnQC f) yieldProgress = withConnection $ \c ->
    run opts (QC $ f c) yieldProgress
