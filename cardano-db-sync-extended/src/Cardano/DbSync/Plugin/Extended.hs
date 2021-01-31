module Cardano.DbSync.Plugin.Extended
  ( extendedDbSyncNodePlugin
  ) where

import qualified Cardano.Db as DB
import           Cardano.DbSync.Plugin.Default (defDbSyncNodePlugin)
import           Cardano.DbSync.Plugin.Epoch (epochPluginInsertBlock, epochPluginOnStartup,
                   epochPluginRollbackBlock)
import           Cardano.Sync (DbSyncNodePlugin (..))

extendedDbSyncNodePlugin :: DbSyncNodePlugin
extendedDbSyncNodePlugin =
  defDbSyncNodePlugin
    { plugOnStartup =
        plugOnStartup defDbSyncNodePlugin
          ++ [\tracer -> fmap Right $ DB.runDbAction (Just tracer) $ epochPluginOnStartup tracer]

    , plugInsertBlock =
        plugInsertBlock defDbSyncNodePlugin
          ++ [\tracer env ledgerStateVar blockDetails -> DB.runDbAction (Just tracer) $ epochPluginInsertBlock tracer env ledgerStateVar blockDetails]

    , plugRollbackBlock =
        plugRollbackBlock defDbSyncNodePlugin
          ++ [epochPluginRollbackBlock]
    }

