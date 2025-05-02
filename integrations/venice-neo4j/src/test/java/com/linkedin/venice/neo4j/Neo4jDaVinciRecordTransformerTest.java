package com.linkedin.venice.neo4j;

import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.SINGLE_FIELD_RECORD_SCHEMA;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.Set;
import org.testng.annotations.Test;


public class Neo4jDaVinciRecordTransformerTest {
  static final int storeVersion = 1;
  static final int partitionId = 0;
  static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  static final String storeName = "test_store";
  private final Set<String> columnsToProject = Collections.emptySet();

  @Test
  public void testRecordTransformer() {
    String tempDir = Utils.getTempDataDirectory().getAbsolutePath();

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction((storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> null)
        .setStoreRecordsInDaVinci(false)
        .build();

    try (Neo4jDaVinciRecordTransformer recordTransformer = new Neo4jDaVinciRecordTransformer(
        storeVersion,
        SINGLE_FIELD_RECORD_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        NAME_RECORD_V1_SCHEMA,
        dummyRecordTransformerConfig,
        tempDir,
        storeName,
        columnsToProject)) {
    }
  }
}
