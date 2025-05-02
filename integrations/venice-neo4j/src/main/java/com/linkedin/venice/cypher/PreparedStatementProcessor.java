package com.linkedin.venice.cypher;

import java.sql.PreparedStatement;
import org.apache.avro.generic.GenericRecord;


/** Populates a {@link PreparedStatement} from Avro key/value records. */
public interface PreparedStatementProcessor {
  void process(GenericRecord key, GenericRecord value, PreparedStatement preparedStatement);
}
