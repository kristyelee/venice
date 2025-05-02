package com.linkedin.venice.neo4j;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.sql.AvroToSQL;
import com.linkedin.venice.sql.PreparedStatementProcessor;
import com.linkedin.venice.utils.concurrent.CloseableThreadLocal;
import com.linkedin.venice.utils.lazy.Lazy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Neo4jDaVinciRecordTransformer
    extends DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> {
  private static final Logger LOGGER = LogManager.getLogger(Neo4jDaVinciRecordTransformer.class);
  private static final String neo4jFilePath = "my_database.neo4j";
  private static final String createViewStatementTemplate = "";
  private static final String dropTableStatementTemplate = "";
  private final String storeNameWithoutVersionInfo;
  private final String versionTableName;
  private final String neo4jUrl;
  private final Set<String> columnsToProject;
  private final CloseableThreadLocal<Connection> connection;
  private final CloseableThreadLocal<PreparedStatement> deletePreparedStatement;
  private final CloseableThreadLocal<PreparedStatement> upsertPreparedStatement;
  private final PreparedStatementProcessor upsertProcessor;
  private final PreparedStatementProcessor deleteProcessor;

  public Neo4jDaVinciRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      String baseDir,
      String storeNameWithoutVersionInfo,
      Set<String> columnsToProject) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
    this.storeNameWithoutVersionInfo = storeNameWithoutVersionInfo;
    this.versionTableName = buildStoreNameWithVersion(storeVersion);
    this.neo4jUrl = "jdbc:neo4j:" + baseDir + "/" + neo4jFilePath;
    this.columnsToProject = columnsToProject;
    String deleteStatement = AvroToSQL.deleteStatement(versionTableName, keySchema);
    String upsertStatement = AvroToSQL.upsertStatement(versionTableName, keySchema, inputValueSchema, columnsToProject);

    try {
      Class.forName("org.neo4j.jdbc.Neo4jDriver");
    } catch (ClassNotFoundException e) {
      throw new VeniceException("Failed to load DB Driver!");
    }

    this.connection = CloseableThreadLocal.withInitial(() -> {
      try {
        return DriverManager.getConnection(neo4jUrl);
      } catch (SQLException e) {
        throw new VeniceException("Failed to connect to DB!", e);
      }
    });
    this.deletePreparedStatement = CloseableThreadLocal.withInitial(() -> {
      try {
        return this.connection.get().prepareStatement(deleteStatement);
      } catch (SQLException e) {
        throw new VeniceException("Failed to create PreparedStatement for: " + deleteStatement, e);
      }
    });
    this.upsertPreparedStatement = CloseableThreadLocal.withInitial(() -> {
      try {
        return this.connection.get().prepareStatement(upsertStatement);
      } catch (SQLException e) {
        throw new VeniceException("Failed to create PreparedStatement for: " + upsertStatement, e);
      }
    });
    this.upsertProcessor = AvroToSQL.upsertProcessor(keySchema, inputValueSchema, columnsToProject);
    this.deleteProcessor = AvroToSQL.deleteProcessor(keySchema);
  }

  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(
      Lazy<GenericRecord> key,
      Lazy<GenericRecord> value,
      int partitionId) {
    // Record transformation happens inside processPut as we need access to the connection object to create the prepared
    // statement
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(Lazy<GenericRecord> key, Lazy<GenericRecord> value, int partitionId) {
    // this.upsertProcessor.process(key.get(), value.get(), this.upsertPreparedStatement.get());
  }

  @Override
  public void processDelete(Lazy<GenericRecord> key, int partitionId) {
    // this.deleteProcessor.process(key.get(), null, this.deletePreparedStatement.get());
  }

  public String buildStoreNameWithVersion(int version) {
    return storeNameWithoutVersionInfo + "_v" + version;
  }

  @Override
  public void close() {
    this.deletePreparedStatement.close();
    this.upsertPreparedStatement.close();
    this.connection.close();
  }
}
