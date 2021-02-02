{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Cardano.DbSync.Era.Shelley.Generic.Rewards
  ( Rewards (..)
  , allegraRewards
  , maryRewards
  , shelleyRewards
  ) where

import           Cardano.DbSync.Era.Shelley.Generic.StakeCred

import           Cardano.Ledger.Era (Crypto)

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import           Ouroboros.Consensus.Cardano.Block (LedgerState (..), StandardAllegra, StandardMary,
                   StandardShelley)
import           Ouroboros.Consensus.Cardano.CanHardFork ()
import           Ouroboros.Consensus.Shelley.Ledger.Block (ShelleyBlock)
import qualified Ouroboros.Consensus.Shelley.Ledger.Ledger as Consensus

import qualified Shelley.Spec.Ledger.BaseTypes as Shelley
import           Shelley.Spec.Ledger.Coin (Coin (..))
import qualified Shelley.Spec.Ledger.Credential as Shelley
import qualified Shelley.Spec.Ledger.Keys as Shelley
import qualified Shelley.Spec.Ledger.LedgerState as Shelley

-- The `ledger-specs` code defines a `RewardUpdate` type that is parameterised over
-- Shelley/Allegra/Mary. This is a huge pain in the neck for `db-sync` so we define a
-- generic one instead.
data Rewards = Rewards
    { rewards :: Map StakeCred Coin
    , orphaned :: Map StakeCred Coin
    }

allegraRewards :: Shelley.Network -> LedgerState (ShelleyBlock StandardAllegra) -> Maybe Rewards
allegraRewards = genericRewards

maryRewards :: Shelley.Network -> LedgerState (ShelleyBlock StandardMary) -> Maybe Rewards
maryRewards = genericRewards

shelleyRewards :: Shelley.Network -> LedgerState (ShelleyBlock StandardShelley) -> Maybe Rewards
shelleyRewards = genericRewards

-- -------------------------------------------------------------------------------------------------

genericRewards :: forall era. Shelley.Network -> LedgerState (ShelleyBlock era) -> Maybe Rewards
genericRewards network lstate =
    fmap cleanup rewardUpdate
  where
    cleanup :: Map (Shelley.Credential 'Shelley.Staking (Crypto era)) Coin -> Rewards
    cleanup rmap =
      let (rm, om) = Map.partitionWithKey validRewardAddress rmap in
      Rewards
        { rewards = Map.mapKeys (toStakeCred network) rm
        , orphaned = Map.mapKeys (toStakeCred network) om
        }
    rewardAccounts :: Shelley.RewardAccounts (Crypto era)
    rewardAccounts =
      Shelley._rewards . Shelley._dstate . Shelley._delegationState . Shelley.esLState
        $ Shelley.nesEs (Consensus.shelleyLedgerState lstate)

    rewardUpdate :: Maybe (Map (Shelley.Credential 'Shelley.Staking (Crypto era)) Coin)
    rewardUpdate =
      Shelley.rs <$> Shelley.strictMaybeToMaybe (Shelley.nesRu $ Consensus.shelleyLedgerState lstate)

    validRewardAddress :: Shelley.Credential 'Shelley.Staking (Crypto era) -> a -> Bool
    validRewardAddress addr _value = Map.member addr rewardAccounts
