{-# LANGUAGE OverloadedStrings #-}

module Cardano.DbSync.Environment
  ( DbSyncEnv (..)
  , LedgerEnv (..)
  , CardanoLedgerState (..)
  , genesisConfigToEnv
  , getLatestPoints
  , validLedgerFileToPoint
  ) where

import           Cardano.Prelude (mapMaybe)

import qualified Cardano.Db as DB

import           Cardano.DbSync.Config.Cardano
import           Cardano.DbSync.Config.Shelley
import           Cardano.DbSync.Config.Types
import           Cardano.DbSync.Error
import           Cardano.DbSync.LedgerState
import           Cardano.Slotting.Slot (SlotNo (..))

import qualified Cardano.Chain.Genesis as Byron
import           Cardano.Crypto.ProtocolMagic

import           Control.Monad.Extra

import           Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo)
import           Ouroboros.Network.Block (Point (..))
import           Ouroboros.Network.Magic (NetworkMagic (..))
import           Ouroboros.Consensus.BlockchainTime.WallClock.Types (SystemStart (..))

import qualified Shelley.Spec.Ledger.BaseTypes as Shelley
import qualified Shelley.Spec.Ledger.Genesis as Shelley

import           Cardano.DbSync.Util (textShow)

data DbSyncEnv = DbSyncEnv
  { envProtocol :: !DbSyncProtocol
  , envNetworkMagic :: !NetworkMagic
  , envSystemStart :: !SystemStart
  , envLedger :: LedgerEnv
  }

mkDbSyncEnv :: ProtocolInfo IO CardanoBlock -> Shelley.Network -> NetworkMagic -> SystemStart -> LedgerStateDir -> IO DbSyncEnv
mkDbSyncEnv protocolInfo network networkMagic systemStart dir = do
    latestSlot <- SlotNo <$> DB.runDbNoLogging DB.queryLatestSlotNo
    ledgerEnv <- mkLedgerEnv protocolInfo dir network latestSlot True
    return $ DbSyncEnv
      { envProtocol = DbSyncProtocolCardano
      , envNetworkMagic = networkMagic
      , envSystemStart = systemStart
      , envLedger = ledgerEnv
      }

genesisConfigToEnv :: LedgerStateDir -> GenesisConfig -> IO (Either DbSyncNodeError DbSyncEnv)
genesisConfigToEnv dir genCfg =
    case genCfg of
      GenesisCardano _ bCfg sCfg
        | unProtocolMagicId (Byron.configProtocolMagicId bCfg) /= Shelley.sgNetworkMagic (scConfig sCfg) ->
            return $ Left . NECardanoConfig $
              mconcat
                [ "ProtocolMagicId ", textShow (unProtocolMagicId $ Byron.configProtocolMagicId bCfg)
                , " /= ", textShow (Shelley.sgNetworkMagic $ scConfig sCfg)
                ]
        | Byron.gdStartTime (Byron.configGenesisData bCfg) /= Shelley.sgSystemStart (scConfig sCfg) ->
            return $ Left . NECardanoConfig $
              mconcat
                [ "SystemStart ", textShow (Byron.gdStartTime $ Byron.configGenesisData bCfg)
                , " /= ", textShow (Shelley.sgSystemStart $ scConfig sCfg)
                ]
        | otherwise -> Right <$> mkDbSyncEnv
                         (mkProtocolInfoCardano genCfg)
                         (Shelley.sgNetworkId (scConfig sCfg))
                         (NetworkMagic (unProtocolMagicId $ Byron.configProtocolMagicId bCfg))
                         (SystemStart (Byron.gdStartTime $ Byron.configGenesisData bCfg))
                         dir

getLatestPoints :: DbSyncEnv -> IO [Point CardanoBlock]
getLatestPoints env = do
    files <- listLedgerStateFilesOrdered (leDir $ envLedger env)
    (validFiles, _) <- partitionM validLedgerFileToPoint files
    return $ mapMaybe fileToPoint validFiles

validLedgerFileToPoint :: LedgerStateFile -> IO Bool
validLedgerFileToPoint lsf = do
    mslot <- DB.runDbNoLogging $ DB.queryBlockSlotNo (lsfHash lsf)
    case mslot of
      Right (Just slot) -> return $ SlotNo slot == lsfSlotNo lsf
      _ -> return False