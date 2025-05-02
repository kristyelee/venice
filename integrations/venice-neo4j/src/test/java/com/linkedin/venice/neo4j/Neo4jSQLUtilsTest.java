package com.linkedin.venice.neo4j;

import com.linkedin.venice.sql.SQLUtilsTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.testng.annotations.Test;


@Test
public class Neo4jSQLUtilsTest extends SQLUtilsTest {
  @Override
  protected Connection getConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:neo4j");
  }
}
