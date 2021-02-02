{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Cardano.DbSync.Era.Shelley.Generic.StakeDist
  ( StakeCred (..)
  , StakeDist (..)
  , allegraStakeDist
  , maryStakeDist
  , shelleyStakeDist
  ) where

import           Cardano.DbSync.Era.Shelley.Generic.StakeCred

import           Cardano.Prelude

import qualified Data.Map.Strict as Map

import           Ouroboros.Consensus.Cardano.Block (LedgerState (..), StandardAllegra, StandardMary,
                   StandardShelley)

import           Ouroboros.Consensus.Shelley.Ledger.Block (ShelleyBlock)
import qualified Ouroboros.Consensus.Shelley.Ledger.Ledger as Consensus

import qualified Shelley.Spec.Ledger.BaseTypes as Shelley
import           Shelley.Spec.Ledger.Coin (Coin (..))
import qualified Shelley.Spec.Ledger.EpochBoundary as Shelley
import qualified Shelley.Spec.Ledger.LedgerState as Shelley


newtype StakeDist
  = StakeDist { unStakeDist :: Map StakeCred Coin }


-- We use '_pstakeSet' here instead of '_pstateMark' because the stake addresses for the
-- later may not have been added to the database yet. That means that when these values
-- are added to the database, the epoch number where they become active is the current
-- epoch plus one.

allegraStakeDist :: Shelley.Network -> LedgerState (ShelleyBlock StandardAllegra) -> StakeDist
allegraStakeDist network =
  StakeDist . Map.mapKeys (toStakeCred network) . Shelley.unStake . Shelley._stake . Shelley._pstakeSet
    . Shelley.esSnapshots . Shelley.nesEs . Consensus.shelleyLedgerState

maryStakeDist :: Shelley.Network -> LedgerState (ShelleyBlock StandardMary) -> StakeDist
maryStakeDist network =
  StakeDist . Map.mapKeys (toStakeCred network) . Shelley.unStake . Shelley._stake . Shelley._pstakeSet
    . Shelley.esSnapshots . Shelley.nesEs . Consensus.shelleyLedgerState

shelleyStakeDist :: Shelley.Network -> LedgerState (ShelleyBlock StandardShelley) -> StakeDist
shelleyStakeDist network =
  StakeDist . Map.mapKeys (toStakeCred network) . Shelley.unStake . Shelley._stake . Shelley._pstakeSet
    . Shelley.esSnapshots . Shelley.nesEs . Consensus.shelleyLedgerState
