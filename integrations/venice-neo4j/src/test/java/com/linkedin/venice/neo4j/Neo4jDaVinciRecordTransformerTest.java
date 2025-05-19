package com.linkedin.venice.neo4j;

import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.SINGLE_FIELD_RECORD_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.DaVinciRecordTransformerUtility;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Collections;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

      assertTrue(recordTransformer.useUniformInputValueSchema());

      Schema keySchema = recordTransformer.getKeySchema();
      assertEquals(keySchema.getType(), Schema.Type.RECORD);

      Schema outputValueSchema = recordTransformer.getOutputValueSchema();
      assertEquals(outputValueSchema.getType(), Schema.Type.RECORD);

      recordTransformer.onStartVersionIngestion(true);

      GenericRecord keyRecord = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
      keyRecord.put("key", "key");
      Lazy<GenericRecord> lazyKey = Lazy.of(() -> keyRecord);

      GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
      valueRecord.put("firstName", "Duck");
      valueRecord.put("lastName", "Goose");
      Lazy<GenericRecord> lazyValue = Lazy.of(() -> valueRecord);

      DaVinciRecordTransformerResult<GenericRecord> transformerResult =
          recordTransformer.transform(lazyKey, lazyValue, partitionId);
      recordTransformer.processPut(lazyKey, lazyValue, partitionId);
      assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.UNCHANGED);
      // Result will be empty when it's UNCHANGED
      assertNull(transformerResult.getValue());
      assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue, partitionId));

      recordTransformer.processDelete(lazyKey, partitionId);

      assertFalse(recordTransformer.getStoreRecordsInDaVinci());

      int classHash = recordTransformer.getClassHash();

      DaVinciRecordTransformerUtility<GenericRecord, GenericRecord> recordTransformerUtility =
          recordTransformer.getRecordTransformerUtility();
      OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);

      assertTrue(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));

      offsetRecord.setRecordTransformerClassHash(classHash);

      assertFalse(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));
    }
  }
}
