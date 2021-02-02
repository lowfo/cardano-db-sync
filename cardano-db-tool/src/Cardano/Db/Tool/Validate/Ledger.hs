module Cardano.Db.Tool.Validate.Ledger
  ( LedgerValidationParams (..)
  , validateLedger
  ) where

import           Control.Monad (when)
import           Control.Monad.Trans.Except.Exit (orDie)
import           Data.Text (Text)
import qualified Data.Text as Text
import           Prelude

import qualified Cardano.Db as DB
import           Cardano.Db.Tool.Validate.Balance (ledgerAddrBalance)
import           Cardano.Db.Tool.Validate.Util

import           Cardano.Sync.Config
import           Cardano.Sync.Error
import           Cardano.Sync.LedgerState
import           Cardano.Sync.Tracing.ToObjectOrphans ()

import           Cardano.Slotting.Slot (SlotNo (..))

import           Ouroboros.Consensus.Cardano.Node ()
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Network.NodeToClient (withIOManager)

data LedgerValidationParams = LedgerValidationParams
  { vpConfigFile :: !ConfigFile
  , vpLedgerStateDir :: !LedgerStateDir
  , vpAddressUtxo :: !Text
  }

validateLedger :: LedgerValidationParams -> IO ()
validateLedger params =
  withIOManager $ \ _ -> do
    enc <- readDbSyncNodeConfig (vpConfigFile params)
    ledgerFiles <- listLedgerStateFilesOrdered (vpLedgerStateDir params)
    slotNo <- SlotNo <$> DB.runDbNoLogging DB.queryLatestSlotNo
    genCfg <- orDie renderDbSyncNodeError $ readCardanoGenesisConfig enc
    validate params genCfg slotNo ledgerFiles

validate :: LedgerValidationParams -> GenesisConfig -> SlotNo -> [LedgerStateFile] -> IO ()
validate params genCfg slotNo ledgerFiles =
    go ledgerFiles True
  where
    go :: [LedgerStateFile] -> Bool -> IO ()
    go [] _ = putStrLn $ redText "No ledger found"
    go (ledgerFile : rest) logFailure = do
      let ledgerSlot = lsfSlotNo ledgerFile
      if ledgerSlot <= slotNo
        then do
          Just state <- loadLedgerStateFromFile genCfg ledgerFile
          validateBalance ledgerSlot (vpAddressUtxo params) state
        else do
          when logFailure . putStrLn $ redText "Ledger is newer than DB. Trying an older ledger."
          go rest False

validateBalance :: SlotNo -> Text -> CardanoLedgerState -> IO ()
validateBalance slotNo addr st = do
  balanceDB <- DB.runDbNoLogging $ DB.queryAddressBalanceAtSlot addr (unSlotNo slotNo)
  let eiBalanceLedger = DB.word64ToAda <$> ledgerAddrBalance addr (ledgerState $ clsState st)
  case eiBalanceLedger of
    Left str -> putStrLn $ redText (Text.unpack str)
    Right balanceLedger ->
      if balanceDB == balanceLedger
        then
          putStrF $ concat
            [ "DB and Ledger balance for address ", Text.unpack addr, " at slot ", show (unSlotNo slotNo)
            , " match (", show balanceLedger, " ada) : ", greenText "ok", "\n"
            ]
        else
          error . redText $ concat
            [ "failed: DB and Ledger balances for address ", Text.unpack addr, " don't match. "
            , "DB value (", show balanceDB, ") /= ledger value (", show balanceLedger, ")."
            ]
