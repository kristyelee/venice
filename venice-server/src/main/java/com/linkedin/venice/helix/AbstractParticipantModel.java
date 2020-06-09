package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.controller.VeniceStateModel;
import java.util.concurrent.TimeUnit;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;


/**
 * An abstraction of Helix State model behavior registry that defines how participants
 * (Storage node) fetch data from Kafka.
 *
 * Helix state model is state machine defined by "states" and transitions (edges from one
 * state to the other).
 *
 * In order to make Helix state machine work, 2 parts are required.
 * 1. A model definition that defines all available states and transitions.
 * 2. A behavior registry that defines what participant should do during transitions before
 * moving to next state.
 *
 * Currently, we support 2 kinds of participant model.
 * 1. PartitionOnlineOfflineModel. Check out {@link VeniceStateModel} for model definition
 * and {@link VenicePartitionStateModel} for behavior registry.
 * 2. LeaderStandbyModel. Check out {@link LeaderStandbySMD} for model definition and
 * {@link LeaderFollowerParticipantModel} for behavior registry.
 */
public abstract class AbstractParticipantModel extends StateModel {
  protected final Logger logger = Logger.getLogger(getClass());
  private static final String STORE_PARTITION_DESCRIPTION_FORMAT = "%s-%d";

  private final StoreIngestionService storeIngestionService;
  private final ReadOnlyStoreRepository metaDataRepo;
  private final StorageService storageService;
  private final VeniceStoreConfig storeConfig;
  private final int partition;
  private final Time time;

  private final String storePartitionDescription;

  public AbstractParticipantModel(StoreIngestionService storeIngestionService, ReadOnlyStoreRepository metaDataRepo,
      StorageService storageService,
      VeniceStoreConfig storeConfig, int partition, Time time) {
    this.storeIngestionService = storeIngestionService;
    this.metaDataRepo = metaDataRepo;
    this.storageService = storageService;
    this.storeConfig = storeConfig;
    this.partition = partition;
    this.time = time;

    this.storePartitionDescription = String
        .format(STORE_PARTITION_DESCRIPTION_FORMAT, storeConfig.getStoreName(), partition);
  }

  protected void executeStateTransition(Message message, NotificationContext context,
      Runnable handler) {
    String from = message.getFromState();
    String to = message.getToState();
    logEntry(from, to, message, context);
    // Change name to indicate which st is occupied this thread.
    Thread.currentThread()
        .setName("Helix-ST-" + message.getResourceName() + "-" + partition + "-" + from + "->" + to);
    try {
      handler.run();
      logCompletion(from, to, message, context);
    } finally {
      // Once st is terminated, change the name to indicate this thread will not be occupied by this st.
      Thread.currentThread().setName("Inactive ST thread.");
    }
  }

  private void logEntry(String from, String to, Message message, NotificationContext context) {
    logger.info(getStorePartitionDescription() + " initiating transition from " + from + " to " + to + " for resource: "
        + getStoreConfig().getStoreName() + " Partition " + partition +
        " invoked with Message " + message + " and context " + context);
  }

  private void logCompletion(String from, String to, Message message, NotificationContext context) {
    logger.info(getStorePartitionDescription() + " completed transition from " + from + " to " + to + " for resource: "
        + getStoreConfig().getStoreName() + " Partition " + partition +
        " invoked with Message " + message + " and context " + context);
  }

  /**
   * Stop the consumption once a replica become ERROR.
   * This function only does clean up when any state transition fails, and it won't retry any state transition.
   */
  @Override
  public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
    executeStateTransition(message, context, ()-> {
      logger.info(getStorePartitionDescription() + " met an error during state transition. Stop the running consumption. Caused by:",
          error.getException());
      /**
       * When state transition fails, we shouldn't remove the corresponding database here since the database could
       * be either recovered by bounce/Helix Reset or completely dropped after going through 'ERROR' to 'DROPPED' state transition.
       */
      stopConsumption();
    });
  }

  /**
   * Handles ERROR->DROPPED transition. Unexpected Transition. Unsubscribe the partition, removes partition's data
   * from local storage and clears the committed offset.
   */
  @Override
  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.ERROR_STATE)
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    executeStateTransition(message, context, ()-> {
      try {
        stopConsumptionAndDropPartitionOnError();
      } catch (Throwable e) {
        // Catch throwable here to ensure state transition is completed to avoid enter into the infinite loop error->dropped->error->....
        logger.error("Met error during the  transition.", e);
      }
    });
  }


  @Override
  public void reset() {}

  /**
   * set up a new store partition and start the ingestion
   */
  protected void setupNewStorePartition(boolean isLeaderFollowerModel) {
    // If given store and partition have already exist in this node, openStoreForNewPartition is idempotent so it
    // will not create them again.
    storageService.openStoreForNewPartition(storeConfig, partition);
    storeIngestionService.startConsumption(storeConfig, partition, isLeaderFollowerModel);
  }

  protected void removePartitionFromStoreGracefully() {
    try {
      // Gracefully drop partition to drain the requests to this partition
      // This method is called during OFFLINE->DROPPED state transition. Due to Zk or other transient issues a store
      // version could miss ONLINE->OFFLINE transition and newer version could come online triggering this transition.
      // Since this removes the storageEngine from the map not doing a un-subscribe and dropping a partition could
      // lead to NPE and other issues.
      // Adding a topic unsubscribe call for those race conditions as a safe-guard before dropping the partition.
      stopConsumption();
      getTime().sleep(TimeUnit.SECONDS.toMillis(getStoreConfig().getPartitionGracefulDropDelaySeconds()));
    } catch (InterruptedException e) {
      throw new VeniceException("Got interrupted during state transition: 'OFFLINE' -> 'DROPPED'", e);
    }
    removePartitionFromStore();
  }

  protected void removePartitionFromStore() {
    try {
      /**
       * Since un-subscription is an asynchronous process, so we would like to make sure current partition is not
       * being consuming before dropping store partition.
       *
       * Otherwise, a {@link com.linkedin.venice.exceptions.PersistenceFailureException} could be thrown here:
       * {@link com.linkedin.venice.store.AbstractStorageEngine#put(Integer, byte[], byte[])}.
       */
      makeSurePartitionIsNotConsuming();
    } catch (Exception e) {
      logger.error("Error waiting for partition to stop consuming", e);
    }
    /**
     * RESET_OFFSET only happens when we want to drop the corresponding database, and this is independent
     * from the topic partition unsubscription.
     */
    getStoreIngestionService().resetConsumptionOffset(getStoreConfig(), partition);

    // Catch exception separately to ensure reset consumption offset would be executed for sure.
    try {
      getStorageService().dropStorePartition(getStoreConfig(), partition);
    } catch (Exception e) {
      logger.error(
          "Error dropping the partition:" + partition + " in store:" + getStoreConfig().getStoreName());
    }
  }

  /**
   * This function is trying to wait for current partition of store stop consuming.
   */
  private void makeSurePartitionIsNotConsuming() throws InterruptedException {
    final int SLEEP_SECONDS = 3;
    final int RETRY_NUM = 100; // 5 mins
    int current = 0;
    while (current++ < RETRY_NUM) {
      if (!getStoreIngestionService().isPartitionConsuming(getStoreConfig(), partition)) {
        return;
      }
      getTime().sleep(SLEEP_SECONDS * Time.MS_PER_SECOND);
    }
    throw new VeniceException("Partition: " + partition + " of store: " + getStoreConfig().getStoreName() +
        " is still consuming after waiting for it to stop for " + RETRY_NUM * SLEEP_SECONDS + " seconds.");
  }

  protected void waitConsumptionCompleted(String resourceName, StateModelNotifier notifier) {
    try {
      int bootstrapToOnlineTimeoutInHours;
      try {
        bootstrapToOnlineTimeoutInHours = getMetaDataRepo()
            .getStore(Version.parseStoreFromKafkaTopicName(resourceName))
            .getBootstrapToOnlineTimeoutInHours();
      } catch (Exception e) {
        logger.warn("Failed to fetch bootstrapToOnlineTimeoutInHours from store config for resource "
            + resourceName + ", using the default value of "
            + Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS + " hours instead");
        bootstrapToOnlineTimeoutInHours = Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
      }
      notifier.waitConsumptionCompleted(resourceName, partition, bootstrapToOnlineTimeoutInHours,
          storeIngestionService.getAggStoreIngestionStats(), storeIngestionService.getAggVersionedStorageIngestionStats());
    } catch (InterruptedException e) {
      String errorMsg =
          "Can not complete consumption for resource:" + resourceName + " partition:" + partition;
      logger.error(errorMsg, e);
      // Please note, after throwing this exception, this node will become ERROR for this resource.
      throw new VeniceException(errorMsg, e);
    }
  }

  protected void stopConsumption() {
    storeIngestionService.stopConsumption(storeConfig, partition);
  }

  protected void stopConsumptionAndDropPartitionOnError() {
    stopConsumption();
    removePartitionFromStore();
  }

  public StoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  public ReadOnlyStoreRepository getMetaDataRepo() {
    return metaDataRepo;
  }

  public StorageService getStorageService() {
    return storageService;
  }

  public VeniceStoreConfig getStoreConfig() {
    return storeConfig;
  }

  public int getPartition() {
    return partition;
  }

  public Time getTime() {
    return time;
  }

  public String getStorePartitionDescription() {
    return storePartitionDescription;
  }
}
