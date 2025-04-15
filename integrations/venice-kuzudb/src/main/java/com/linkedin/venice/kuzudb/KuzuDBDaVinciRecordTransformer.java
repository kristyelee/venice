package com.linkedin.venice.kuzudb;

//import com.kuzudb.*;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.sql.AvroToSQL;
import com.linkedin.venice.sql.PreparedStatementProcessor;
import com.linkedin.venice.sql.SQLUtils;
import com.linkedin.venice.utils.concurrent.CloseableThreadLocal;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.sparkproject.jetty.client.api.Connection;


public class KuzuDBDaVinciRecordTransformer
    extends DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> {
  private static final Logger LOGGER = LogManager.getLogger(KuzuDBDaVinciRecordTransformer.class);
  private static final String kuzuDBFilePath = "my_database.kuzudb";
  private static final String createViewStatementTemplate = "CREATE OR REPLACE VIEW %s AS SELECT * FROM %s;";
  private static final String dropTableStatementTemplate = "DROP TABLE %s;";
  private final String storeNameWithoutVersionInfo;
  private final String versionTableName;
  private final String kuzuDBUrl;
  private final Set<String> columnsToProject;
  private final CloseableThreadLocal<Connection> connection;
  private final CloseableThreadLocal<PreparedStatement> deleteCypherPreparedStatement;
  private final CloseableThreadLocal<PreparedStatement> upsertCypherPreparedStatement;
  private final PreparedStatementProcessor upsertCypherProcessor;
  private final PreparedStatementProcessor deleteCypherProcessor;
  private final String deleteStatement;
  private final String upsertStatement;

  public KuzuDBDaVinciRecordTransformer(
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
    this.kuzuDBUrl = "kuzudb:" + baseDir + "/" + kuzuDBFilePath;
    this.columnsToProject = columnsToProject;
    this.deleteStatement = deleteStatementInCypher(versionTableName, keySchema);
    this.upsertStatement = upsertStatementInCypher(versionTableName, keySchema, inputValueSchema, columnsToProject);
    this.connection = CloseableThreadLocal.withInitial(() -> {
      return null;
      // try {
      // // TODO: Find out how KuzuDB is connected and faciliates connection;
      // Database db = new Database(":memory");
      // Connection conn = new Connection(db);
      // return conn;
      // return DriverManager.getConnection(kuzuDBUrl);
      // } catch (SQLException e) {
      // throw new VeniceException("Failed to connect to DB!", e);
      // }
    });

    // this.deleteCypherPreparedStatement = CloseableThreadLocal.withInitial(() -> {
    // try {
    // return this.connection.get().prepareStatement(deleteStatement);
    // } catch (SQLException e) {
    // throw new VeniceException("Failed to create PreparedStatement for: " + deleteStatement, e);
    // }
    // });
    // this.upsertCypherPreparedStatement = CloseableThreadLocal.withInitial(() -> {
    // try {
    // return this.connection.get().prepareStatement(upsertStatement);
    // } catch (SQLException e) {
    // throw new VeniceException("Failed to create PreparedStatement for: " + upsertStatement, e);
    // }
    // });
    this.upsertCypherProcessor = AvroToSQL.upsertProcessor(keySchema, inputValueSchema, columnsToProject);
    this.deleteCypherProcessor = AvroToSQL.deleteProcessor(keySchema);
  }

  public String cypherDelete(String SQLStatement) {
    return "";
  }

  public String cypherMatch(String SQLStatement) {
    return "";
  }

  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(Lazy<GenericRecord> key, Lazy<GenericRecord> value) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(Lazy<GenericRecord> key, Lazy<GenericRecord> value) {
    this.upsertCypherProcessor.process(key.get(), value.get(), this.upsertCypherPreparedStatement.get());
  }

  @Override
  public void processDelete(Lazy<GenericRecord> key) {
    this.deleteCypherProcessor.process(key.get(), null, this.deleteCypherPreparedStatement.get());
  }

  @Nonnull
    public static String deleteStatementInCypher(@Nonnull String tableName, @Nonnull Schema keySchema) {
        boolean is_node = keySchema.getFields().size() == 1;
        StringBuffer stringBuffer = new StringBuffer();

        boolean firstColumn = true;
        if (is_node) {
            stringBuffer.append("MATCH (u: " + SQLUtils.cleanTableName(tableName) + ") WHERE ");
            for (Schema.Field field : keySchema.getFields()) {
//                JDBCType correspondingType = getCorrespondingType(field);
//                if (correspondingType == null) {
//                    // skipped field.
//                    throw new IllegalArgumentException("All types from the key schema must be supported, but field '" + field.name() + "' is of type: " + field.schema().getType());
//                }

            stringBuffer.append("u." + SQLUtils.cleanColumnName(field.name()));
            stringBuffer.append(" = ? ");
            }
        stringBuffer.append("DETACH DELETE u;");
        }
        else {
            String from_label = null;
            String to_label = null;
            String from_key = null;
            String to_key = null;

            int index = 0;
            for (Schema.Field field = (Schema.Field) keySchema.getFields()) {
//                JDBCType correspondingType = getCorrespondingType(field);
//                if (correspondingType == null) {
//                    throw new IllegalArgumentException("All types from the key schema must be supported, but field '" + field.name() + "' is of type: " + field.schema().getType());
//                }

                if (index == 0)
                    from_label = field.name();
                else if (index == 1)
                    from_key = field.name();
                else if (index == 2)
                    to_label = field.name();
                else if (index == 3)
                    to_key = field.name();
                else
                    throw new IllegalArgumentException(
                            "key schema has more than 4 fields, field'" + field.name() + " illegal"
                    );
                index = index + 1;
            }
            // Example: MATCH (u:User)-[f:Follows] -> (u1: User)
            //          WHERE u.name = 'Adam' and u1.name = 'Karissa'
            //          DELETE f;

            //stringBuffer.append("MATCH (u:" + from_label + ")-[f:" + SQLUtils.cleanTableName(tableName) + "]->(u1: " + to_label + ") ");
            stringBuffer.append("MATCH (u:" + from_label + ")-[f:" + tableName + "]->(u1: " + to_label + ") ");
            stringBuffer.append("WHERE u." + from_key + "= ? AND u1." + to_key + "= ?");
            stringBuffer.append("DELETE f;");

        }

        return stringBuffer.toString();
    }

  @Nonnull
  public static String upsertStatementInCypher(
      @Nonnull String tableName,
      @Nonnull Schema keySchema,
      @Nonnull Schema valueSchema,
      @Nonnull Set<String> columnsToProject) {
    // Set<Schema.Field> allColumns = combineColumns(keySchema, valueSchema, columnsToProject);
    // StringBuffer stringBuffer = new StringBuffer();
    // stringBuffer.append("INSERT OR REPLACE INTO " + SQLUtils.cleanTableName(tableName) + " VALUES (");
    // boolean firstColumn = true;
    //
    // for (Schema.Field field: allColumns) {
    // JDBCType correspondingType = getCorrespondingType(field);
    // if (correspondingType == null) {
    // // Skipped field.
    // continue;
    // }
    //
    // if (firstColumn) {
    // firstColumn = false;
    // } else {
    // stringBuffer.append(", ");
    // }
    //
    // stringBuffer.append("?");
    // }
    // stringBuffer.append(");");
    //
    // return stringBuffer.toString();
    return "";
  }

  @Override
  public void close() throws IOException {
    this.deleteCypherPreparedStatement.close();
    this.upsertCypherPreparedStatement.close();
    // this.connection.close();
  }

  public String buildStoreNameWithVersion(int version) {
    return storeNameWithoutVersionInfo + "_v" + version;
  }
}
