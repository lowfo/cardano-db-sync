{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.DbSync.Plugin.Epoch
  ( epochPluginOnStartup
  , epochPluginInsertBlock
  , epochPluginRollbackBlock
  ) where

import           Cardano.Prelude hiding (from, on, replace)

import           Cardano.BM.Trace (Trace, logError, logInfo)

import qualified Cardano.Chain.Block as Byron
import           Cardano.Slotting.Slot (EpochNo (..), SlotNo (..))

import           Control.Monad.Logger (LoggingT)
import           Control.Monad.Trans.Control (MonadBaseControl)

import           Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import qualified Data.Time.Clock as Time

import           Database.Esqueleto (Value (..), desc, from, limit, orderBy, select, val, where_,
                   (==.), (^.))

import           Database.Persist.Class (replace)
import           Database.Persist.Sql (SqlBackend)

import           Cardano.Db (EntityField (..), EpochId)
import qualified Cardano.Db as DB

import           Cardano.Sync.Config.Types
import           Cardano.Sync.Error
import           Cardano.Sync.LedgerState
import           Cardano.Sync.Types
import           Cardano.Sync.Util

import           Ouroboros.Consensus.Byron.Ledger (ByronBlock (..))
import           Ouroboros.Consensus.Cardano.Block (HardForkBlock (..))

import           System.IO.Unsafe (unsafePerformIO)

-- Populating the Epoch table has two mode:
--  * SyncLagging: when the node is far behind the chain tip and is just updating the DB. In this
--    mode, the row for an epoch is only calculated and inserted when at the end of the epoch.
--  * Following: When the node is at or close to the chain tip, the row for a given epoch is
--    updated on each new block.
--
-- When in syncing mode, the row for the current epoch being synced may be incorrect.


epochPluginOnStartup :: Trace IO Text -> ReaderT SqlBackend (LoggingT IO) ()
epochPluginOnStartup trce = do
    liftIO . logInfo trce $ "epochPluginOnStartup: Checking"
    mlbe <- queryLatestEpochNo
    case mlbe of
      Nothing ->
        pure ()
      Just lbe -> do
        let backOne = if lbe == 0 then 0 else lbe - 1
        liftIO $ atomicWriteIORef latestCachedEpochVar (Just backOne)

epochPluginInsertBlock
    :: Trace IO Text -> DbSyncEnv -> LedgerStateVar -> BlockDetails
    -> ReaderT SqlBackend (LoggingT IO) (Either DbSyncNodeError ())
epochPluginInsertBlock trce _env _ledgerState (BlockDetails cblk details) = do
    case cblk of
      BlockByron bblk ->
        case byronBlockRaw bblk of
          Byron.ABOBBoundary {} ->
            -- For the OBFT era there are no boundary blocks so we ignore them even in
            -- the Ouroboros Classic era.
            pure $ Right ()

          Byron.ABOBBlock _blk ->
            insertBlock trce details
      BlockShelley _sblk -> epochUpdate
      BlockAllegra _ablk -> epochUpdate
      BlockMary _mblk -> epochUpdate

  where
    -- What we do here is completely independent of Shelley/Allegra/Mary eras.
    epochUpdate :: ReaderT SqlBackend (LoggingT IO) (Either DbSyncNodeError ())
    epochUpdate = do
      when (sdSlotTime details > sdCurrentTime details) $
        liftIO . logError trce $ mconcat
          [ "Slot time '", textShow (sdSlotTime details) ,  "' is in the future" ]
      insertBlock trce details

-- Nothing to be done here.
-- Rollback will take place in the Default plugin and the epoch table will just be recalculated.
epochPluginRollbackBlock :: Trace IO Text -> SlotNo -> IO (Either DbSyncNodeError ())
epochPluginRollbackBlock _ _ = pure $ Right ()

-- -------------------------------------------------------------------------------------------------

insertBlock
    :: Trace IO Text -> SlotDetails
    -> ReaderT SqlBackend (LoggingT IO) (Either DbSyncNodeError ())
insertBlock trce details = do
  mLatestCachedEpoch <- liftIO $ readIORef latestCachedEpochVar
  let lastCachedEpoch = fromMaybe 0 mLatestCachedEpoch
      epochNum = unEpochNo (sdEpochNo details)

  -- These cases are listed from the least likey to occur to the most
  -- likley to keep the logic sane.

  if  | epochNum > 0 && isNothing mLatestCachedEpoch ->
          updateEpochNum 0 trce
      | epochNum >= lastCachedEpoch + 2 ->
          updateEpochNum (lastCachedEpoch + 1) trce
      | getSyncStatus details == SyncFollowing ->
          -- Following the chain very closely.
          updateEpochNum epochNum trce
      | otherwise ->
          pure $ Right ()

-- -------------------------------------------------------------------------------------------------

{-# NOINLINE latestCachedEpochVar #-}
latestCachedEpochVar :: IORef (Maybe Word64)
latestCachedEpochVar = unsafePerformIO $ newIORef Nothing -- Gets updated later.

updateEpochNum :: (MonadBaseControl IO m, MonadIO m) => Word64 -> Trace IO Text -> ReaderT SqlBackend m (Either DbSyncNodeError ())
updateEpochNum epochNum trce = do
    DB.transactionCommit
    mid <- queryEpochId epochNum
    res <- maybe insertEpoch updateEpoch mid
    liftIO $ atomicWriteIORef latestCachedEpochVar (Just epochNum)
    pure res
  where
    updateEpoch :: MonadIO m => EpochId -> ReaderT SqlBackend m (Either DbSyncNodeError ())
    updateEpoch epochId = do
      mEpoch <- DB.queryCalcEpochEntry epochNum
      epoch <- maybe (liftIO calcEpochFromHistory) pure mEpoch
      Right <$> replace epochId epoch

    insertEpoch :: (MonadBaseControl IO m, MonadIO m) => ReaderT SqlBackend m (Either DbSyncNodeError ())
    insertEpoch = do
      mEpoch <- DB.queryCalcEpochEntry epochNum
      liftIO . logInfo trce $ "epochPluginInsertBlock: epoch " <> textShow epochNum
      epoch <- maybe (liftIO calcEpochFromHistory) pure mEpoch
      void $ DB.insertEpoch epoch
      pure $ Right ()

    -- Really do not think this can happen but this is better than an error.
    calcEpochFromHistory :: IO DB.Epoch
    calcEpochFromHistory = do
      now <- Time.getCurrentTime
      pure $ DB.Epoch 0 (DB.DbLovelace 0) 0 0 epochNum now now

-- -------------------------------------------------------------------------------------------------

-- | Get the PostgreSQL row index (EpochId) that matches the given epoch number.
queryEpochId :: MonadIO m => Word64 -> ReaderT SqlBackend m (Maybe EpochId)
queryEpochId epochNum = do
  res <- select . from $ \ epoch -> do
            where_ (epoch ^. DB.EpochNo ==. val epochNum)
            pure (epoch ^. EpochId)
  pure $ unValue <$> listToMaybe res

-- | Get the epoch number of the most recent epoch in the Epoch table.
queryLatestEpochNo :: MonadIO m => ReaderT SqlBackend m (Maybe Word64)
queryLatestEpochNo = do
  res <- select . from $ \ epoch -> do
            orderBy [desc (epoch ^. DB.EpochNo)]
            limit 1
            pure (epoch ^. DB.EpochNo)
  pure $ unValue <$> listToMaybe res
