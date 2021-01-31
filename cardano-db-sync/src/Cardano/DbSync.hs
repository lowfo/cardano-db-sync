{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Cardano.DbSync
  ( ConfigFile (..)
  , DbSyncCommand (..)
  , DbSyncNodeParams (..)
  , DbSyncNodePlugin (..)
  , GenesisFile (..)
  , LedgerStateDir (..)
  , NetworkName (..)
  , SocketPath (..)
  , DB.MigrationDir (..)

  , defDbSyncNodePlugin
  , runDbSyncNode
  ) where

import           Cardano.Prelude hiding (Nat, option, (%))

import           Control.Monad.Trans.Maybe

import           Cardano.BM.Trace (Trace)

import qualified Cardano.Db as DB

import           Cardano.DbSync.Era
import           Cardano.DbSync.Plugin.Default (defDbSyncNodePlugin)
import           Cardano.DbSync.Rollback (unsafeRollback)

import           Cardano.Sync
import           Cardano.Sync.Tracing.ToObjectOrphans ()


runDbSyncNode :: Trace IO Text -> DbSyncNodePlugin -> DbSyncNodeParams -> IO ()
runDbSyncNode trce plugin enp = do

    -- For testing and debugging.
    case enpMaybeRollback enp of
      Just slotNo -> void $ unsafeRollback trce slotNo
      Nothing -> pure ()

    -- The base @DataLayer@.
    let cardanoSyncDataLayer =
            CardanoSyncDataLayer
                { csdlGetSlotHash = DB.runDbIohkLogging trce . DB.querySlotHash

                , csdlGetLatestBlock = runMaybeT $ do
                    block <- MaybeT $ DB.runDbNoLogging DB.queryLatestBlock
                    return $ Block
                        { bHash = DB.blockHash block
                        , bEpochNo = DB.blockEpochNo block
                        , bSlotNo = DB.blockSlotNo block
                        , bBlockNo = DB.blockBlockNo block
                        }
                }

    runSyncNode cardanoSyncDataLayer plugin enp insertValidateGenesisDist


