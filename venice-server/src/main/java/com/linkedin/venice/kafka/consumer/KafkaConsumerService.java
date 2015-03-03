package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.offsets.BdbOffsetManager;
import com.linkedin.venice.kafka.consumer.offsets.OffsetManager;
import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;


/**
 * Acts as the running Kafka interface to Venice. "Manages the consumption of Kafka partitions for each kafka topic
 * consumed by this node.
 */
public class KafkaConsumerService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(KafkaConsumerService.class.getName());

  private final StoreRepository storeRepository;
  private final VeniceConfigService veniceConfigService;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  private final VeniceServerConfig veniceServerConfig;
  private final OffsetManager offsetManager;

  /**
   * A repository of kafka topic to their corresponding partitions and the kafka consumer tasks. This may be used in
   * future for monitoring purposes. etc.
   * TODO: Make this a concurrent map if atomicity is needed in future
   */
  private final Map<String, Map<Integer, SimpleKafkaConsumerTask>> topicNameToPartitionIdAndKafkaConsumerTasksMap;

  private ExecutorService consumerExecutorService;

  public KafkaConsumerService(StoreRepository storeRepository, VeniceConfigService veniceConfigService,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepository) {
    super("kafka-consumer-service");
    this.storeRepository = storeRepository;
    this.veniceConfigService = veniceConfigService;
    this.veniceServerConfig = veniceConfigService.getVeniceServerConfig();
    this.partitionNodeAssignmentRepository = partitionNodeAssignmentRepository;
    this.topicNameToPartitionIdAndKafkaConsumerTasksMap = new HashMap<String, Map<Integer, SimpleKafkaConsumerTask>>();
    if (veniceServerConfig.isEnableKafkaConsumersOffsetManagement()) {
      this.offsetManager = (veniceServerConfig.getOffsetManagerType().equals("bdb") ? new BdbOffsetManager(
          veniceConfigService.getVeniceClusterConfig())
          : null);  // TODO later make this into a switch case type when there is more than one implementation
      if (this.offsetManager == null) {
        throw new VeniceException("OffsetManager enabled but not defined!");
      }
    } else {
      this.offsetManager = null;
    }
  }

  @Override
  public void startInner()
      throws VeniceException {
    logger.info("Starting all kafka consumer tasks on node: " + veniceServerConfig.getNodeId());
    consumerExecutorService = Executors.newCachedThreadPool();
    for (VeniceStoreConfig storeConfig : veniceConfigService.getAllStoreConfigs().values()) {
      registerKafkaConsumers(storeConfig);
    }
    logger.info("All kafka consumer tasks started.");
  }

  /**
   * create and maintain a Kafka simple consumer for each of the partitions in the Kafka topic consumed by this Venice
   * server
   *
   * @param storeConfig configs for the Venice store
   */
  public void registerKafkaConsumers(VeniceStoreConfig storeConfig) {

    /**
     * TODO Make sure that admin service or any other service when registering Kafka consumers ensures that there are
     * no active consumers for the topic/partition
     */
    String topic = storeConfig.getStoreName();
    Map<Integer, SimpleKafkaConsumerTask> partitionIdToKafkaConsumerTaskMap;
    if (!this.topicNameToPartitionIdAndKafkaConsumerTasksMap.containsKey(topic)) {
      partitionIdToKafkaConsumerTaskMap = new HashMap<Integer, SimpleKafkaConsumerTask>();
      this.topicNameToPartitionIdAndKafkaConsumerTasksMap.put(topic, partitionIdToKafkaConsumerTaskMap);
    }
    partitionIdToKafkaConsumerTaskMap = this.topicNameToPartitionIdAndKafkaConsumerTasksMap.get(topic);
    for (int partitionId : partitionNodeAssignmentRepository
        .getLogicalPartitionIds(topic, veniceServerConfig.getNodeId())) {
      SimpleKafkaConsumerTask kafkaConsumerTask = getConsumerTask(storeConfig, partitionId);
      consumerExecutorService.submit(kafkaConsumerTask);
      partitionIdToKafkaConsumerTaskMap.put(partitionId, kafkaConsumerTask);
    }
    this.topicNameToPartitionIdAndKafkaConsumerTasksMap.put(topic, partitionIdToKafkaConsumerTaskMap);
  }

  /**
   * Given a topic and partition id, returns a consumer task that is configured and tied to that specific topic and partition
   * @param storeConfig  - These are configs specific to a Kafka topic.
   * @param partition - The specific kafka partition id
   * @return
   */
  public SimpleKafkaConsumerTask getConsumerTask(VeniceStoreConfig storeConfig, int partition) {
    return new SimpleKafkaConsumerTask(storeConfig, storeRepository.getLocalStorageEngine(storeConfig.getStoreName()),
        partition, offsetManager);
  }

  @Override
  public void stopInner() {
    logger.info("Shutting down Kafka consumer service for node: " + veniceServerConfig.getNodeId());
    if (consumerExecutorService != null) {
      consumerExecutorService.shutdown();
    }
    logger.info("Shut down complete");
  }
}
