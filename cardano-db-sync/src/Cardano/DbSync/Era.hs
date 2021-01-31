{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Cardano.DbSync.Era
  ( insertValidateGenesisDist
  ) where

import           Cardano.Prelude

import           Cardano.BM.Data.Trace (Trace)

import           Cardano.Sync.Config
import           Cardano.Sync.Error

import qualified Cardano.DbSync.Era.Byron.Genesis as Byron
import qualified Cardano.DbSync.Era.Shelley.Genesis as Shelley

insertValidateGenesisDist
    :: Trace IO Text
    -> NetworkName
    -> GenesisConfig
    -> ExceptT DbSyncNodeError IO ()
insertValidateGenesisDist trce nname genCfg =
  case genCfg of
    GenesisCardano _ bCfg sCfg -> do
      Byron.insertValidateGenesisDist trce (unNetworkName nname) bCfg
      Shelley.insertValidateGenesisDist trce (unNetworkName nname) (scConfig sCfg)
