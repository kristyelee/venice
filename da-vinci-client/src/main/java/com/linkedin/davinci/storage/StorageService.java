package com.linkedin.davinci.storage;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.store.StorageEngineFactory;
import com.linkedin.davinci.store.blackhole.BlackHoleStorageEngineFactory;
import com.linkedin.davinci.store.memory.InMemoryStorageEngineFactory;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.log4j.Logger;
import org.rocksdb.Statistics;

import static com.linkedin.venice.meta.PersistenceType.*;


/**
 * Storage interface to Venice Server. Manages creation and deletion of of Storage engines
 * and Partitions.
 *
 * Use StorageEngineRepository, if read only access is desired for the Storage Engines.
 */
public class StorageService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(StorageService.class);

  private final StorageEngineRepository storageEngineRepository;
  private final VeniceConfigLoader configLoader;
  private final VeniceServerConfig serverConfig;
  private final Map<PersistenceType, StorageEngineFactory> persistenceTypeToStorageEngineFactoryMap;
  private final AggVersionedStorageEngineStats aggVersionedStorageEngineStats;
  private final RocksDBMemoryStats rocksDBMemoryStats;
  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;


  public StorageService(
      VeniceConfigLoader configLoader,
      AggVersionedStorageEngineStats storageEngineStats,
      RocksDBMemoryStats rocksDBMemoryStats,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {

    String dataPath = configLoader.getVeniceServerConfig().getDataBasePath();
    if (!Utils.directoryExists(dataPath)) {
      if (!configLoader.getVeniceServerConfig().isAutoCreateDataPath()) {
        throw new VeniceException(
            "Data directory '" + dataPath + "' does not exist and " + ConfigKeys.AUTOCREATE_DATA_PATH + " is disabled.");
      }

      File dataDir = new File(dataPath);
      logger.info("Creating data directory " + dataDir.getAbsolutePath() + ".");
      dataDir.mkdirs();
    }

    this.configLoader = configLoader;
    this.serverConfig = configLoader.getVeniceServerConfig();
    this.storageEngineRepository = new StorageEngineRepository();

    this.persistenceTypeToStorageEngineFactoryMap = new HashMap<>();
    this.aggVersionedStorageEngineStats = storageEngineStats;
    this.rocksDBMemoryStats = rocksDBMemoryStats;
    this.storeVersionStateSerializer = storeVersionStateSerializer;
    this.partitionStateSerializer = partitionStateSerializer;
    initInternalStorageEngineFactories();
    restoreAllStores(configLoader);
  }

  /**
   * Initialize all the internal storage engine factories.
   * Please add it here if you want to add more.
   */
  private void initInternalStorageEngineFactories() {
    persistenceTypeToStorageEngineFactoryMap.put(IN_MEMORY, new InMemoryStorageEngineFactory(serverConfig));
    persistenceTypeToStorageEngineFactoryMap.put(ROCKS_DB, new RocksDBStorageEngineFactory(serverConfig, rocksDBMemoryStats,
        storeVersionStateSerializer, partitionStateSerializer));
    persistenceTypeToStorageEngineFactoryMap.put(BLACK_HOLE, new BlackHoleStorageEngineFactory());
  }

  private void restoreAllStores(VeniceConfigLoader configLoader) {
    logger.info("Start restoring all the stores persisted previously");
    for (Map.Entry<PersistenceType, StorageEngineFactory> entry : persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      PersistenceType pType = entry.getKey();
      StorageEngineFactory factory = entry.getValue();
      logger.info("Start restoring all the stores with type: " + pType);
      Set<String> storeNames = factory.getPersistedStoreNames();
      for (String storeName : storeNames) {
        logger.info("Start restoring store: " + storeName + " with type: " + pType);
        /**
         * Setup store-level persistence type based on current database setup.
         */
        VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName, pType);
        // Load the metadata & data restore settings from config loader.
        storeConfig.setRestoreMetadataPartition(configLoader.getCombinedProperties().getBoolean(ConfigKeys.SERVER_RESTORE_METADATA_PARTITION_ENABLED, true));
        storeConfig.setRestoreDataPartitions(configLoader.getCombinedProperties().getBoolean(ConfigKeys.SERVER_RESTORE_DATA_PARTITIONS_ENABLED, true));
        AbstractStorageEngine storageEngine = openStore(storeConfig);
        Set<Integer> partitionIds = storageEngine.getPartitionIds();

        logger.info("Loaded the following partitions: " + Arrays.toString(partitionIds.toArray()) + ", for store: " + storeName);
        logger.info("Done restoring store: " + storeName + " with type: " + pType);
      }
      logger.info("Done restoring all the stores with type: " + pType);
    }
    logger.info("Done restoring all the stores persisted previously");
  }

  // TODO Later change to Guice instead of Java reflections
  // This method can also be called from an admin service to add new store.

  public synchronized AbstractStorageEngine openStoreForNewPartition(VeniceStoreConfig storeConfig, int partitionId) {

    AbstractStorageEngine engine = openStore(storeConfig);
    synchronized (engine) {
      if (!engine.containsPartition(partitionId)) {
        engine.addStoragePartition(partitionId);
      }
    }
    return engine;
  }

  /**
   * This method should ideally be Private, but marked as public for validating the result.
   *
   * @param storeConfig StoreConfig of the store.
   * @return Factory corresponding to the store.
   */
  public StorageEngineFactory getInternalStorageEngineFactory(VeniceStoreConfig storeConfig) {
    PersistenceType persistenceType = storeConfig.getStorePersistenceType();
    // Instantiate the factory for this persistence type if not already present
    if (persistenceTypeToStorageEngineFactoryMap.containsKey(persistenceType)) {
      return persistenceTypeToStorageEngineFactoryMap.get(persistenceType);
    }

    throw new VeniceException("Unrecognized persistence type " + persistenceType);
  }

  public Optional<Statistics> getRocksDBAggregatedStatistics() {
    if (persistenceTypeToStorageEngineFactoryMap.containsKey(ROCKS_DB)) {
      return ((RocksDBStorageEngineFactory)persistenceTypeToStorageEngineFactoryMap.get(ROCKS_DB)).getAggStatistics();
    }
    return Optional.empty();
  }


  /**
   * Creates a StorageEngineFactory for the persistence type if not already present.
   * Creates a new storage engine for the given store in the factory and registers the storage engine with the store repository.
   *
   * @param storeConfig   The store specific properties
   * @return StorageEngine that was created for the given store definition.
   */
  private synchronized AbstractStorageEngine openStore(VeniceStoreConfig storeConfig) {
    String storeName = storeConfig.getStoreName();
    AbstractStorageEngine engine = storageEngineRepository.getLocalStorageEngine(storeName);
    if (engine != null) {
      return engine;
    }

    /**
     * For new store, it will use the storage engine configured in host level if it is not known.
     */
    if (!storeConfig.isStorePersistenceTypeKnown()) {
      storeConfig.setStorePersistenceType(storeConfig.getPersistenceType());
    }

    logger.info("Creating/Opening Storage Engine " + storeName + " with type: " + storeConfig.getStorePersistenceType());
    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    engine = factory.getStorageEngine(storeConfig);
    storageEngineRepository.addLocalStorageEngine(engine);
    // Setup storage engine stats
    aggVersionedStorageEngineStats.setStorageEngine(storeName, engine);

    return engine;
  }

  /**
   * Removes the Store, Partition from the Storage service.
   */
  public synchronized void dropStorePartition(VeniceStoreConfig storeConfig, int subPartition) {
    String kafkaTopic = storeConfig.getStoreName();

    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaTopic);
    if (storageEngine == null) {
      logger.warn("Storage engine " + kafkaTopic + " does not exist, ignoring drop partition request.");
      return;
    }
    storageEngine.dropPartition(subPartition);
    Set<Integer> subPartitions = storageEngine.getPartitionIds();
    logger.info("Dropped sub-partition " + subPartition + " of " + kafkaTopic + ", subPartitions=" + subPartitions);

    if (subPartitions.isEmpty()) {
      removeStorageEngine(kafkaTopic);
    }
  }

  public synchronized void removeStorageEngine(String kafkaTopic) {
    AbstractStorageEngine<?> storageEngine = getStorageEngineRepository().removeLocalStorageEngine(kafkaTopic);
    if (storageEngine == null) {
      logger.warn("Storage engine " + kafkaTopic + " does not exist, ignoring remove request.");
      return;
    }
    storageEngine.drop();

    VeniceStoreConfig storeConfig = configLoader.getStoreConfig(kafkaTopic);
    storeConfig.setStorePersistenceType(storageEngine.getType());

    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    factory.removeStorageEngine(storageEngine);
  }

  public synchronized void closeStorageEngine(String kafkaTopic) {
    AbstractStorageEngine<?> storageEngine = getStorageEngineRepository().removeLocalStorageEngine(kafkaTopic);
    if (storageEngine == null) {
      logger.warn("Storage engine " + kafkaTopic + " does not exist, ignoring close request.");
      return;
    }
    storageEngine.close();

    VeniceStoreConfig storeConfig = configLoader.getStoreConfig(kafkaTopic);
    storeConfig.setStorePersistenceType(storageEngine.getType());

    StorageEngineFactory factory = getInternalStorageEngineFactory(storeConfig);
    factory.closeStorageEngine(storageEngine);
  }

  public void cleanupAllStores(VeniceConfigLoader configLoader) {
    // Load local storage and delete them safely.
    // TODO Just clean the data dir in case loading and deleting is too slow.
    restoreAllStores(configLoader);
    logger.info("Start cleaning up all the stores persisted previously");
    storageEngineRepository.getAllLocalStorageEngines().stream().forEach(storageEngine -> {
      String storeName = storageEngine.getName();
      logger.info("Start deleting store: " + storeName);
      Set<Integer> partitionIds = storageEngine.getPartitionIds();
      for (Integer partitionId : partitionIds) {
        dropStorePartition(configLoader.getStoreConfig(storeName), partitionId);
      }
      logger.info("Deleted store: " + storeName);
    });
    logger.info("Done cleaning up all the stores persisted previously");
  }

  public StorageEngineRepository getStorageEngineRepository() {
    return storageEngineRepository;
  }

  public AbstractStorageEngine getStorageEngine(String kafkaTopic) {
    return getStorageEngineRepository().getLocalStorageEngine(kafkaTopic);
  }

  @Override
  public boolean startInner() throws Exception {
    // After Storage Node starts, Helix controller initiates the state transition for the Stores that
    // should be consumed/served by the router.

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner()
      throws VeniceException {
    VeniceException lastException = null;
    try {
      this.storageEngineRepository.close();
    } catch (VeniceException e) {
      lastException = e;
    }

    /*Close all storage engine factories */
    for (Map.Entry<PersistenceType, StorageEngineFactory> storageEngineFactory : persistenceTypeToStorageEngineFactoryMap.entrySet()) {
      PersistenceType factoryType =  storageEngineFactory.getKey();
      logger.info("Closing " + factoryType + " storage engine factory");
      try {
        storageEngineFactory.getValue().close();
      } catch (VeniceException e) {
        logger.error("Error closing " + factoryType, e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }
}