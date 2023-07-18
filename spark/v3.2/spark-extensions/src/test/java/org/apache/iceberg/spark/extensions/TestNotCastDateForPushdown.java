/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.extensions;

import java.io.IOException;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestNotCastDateForPushdown extends SparkExtensionsTestBase {

  private final String icebergTableName = tableName + "_iceberg";
  private final String hiveTableName = tableName + "_hive";

  public TestNotCastDateForPushdown(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", icebergTableName);
    sql("DROP TABLE IF EXISTS %s", hiveTableName);
  }

  @Test
  public void testIcebergTableOneTable() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    sql(
        "CREATE TABLE %s (id STRING, dt STRING) USING iceberg partitioned by (dt) LOCATION '%s'",
        icebergTableName, temp.newFolder().toString());

    // Failure cases
    String errorMessagePrefix = "Detected StringType partition filter comparison with DateType";
    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM %s WHERE dt = date '2022-01-01'", icebergTableName))
        .isInstanceOf(Exception.class)
        .hasMessageStartingWith(errorMessagePrefix);

    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM %s WHERE dt >= date '2022-01-01'", icebergTableName))
        .isInstanceOf(Exception.class)
        .hasMessageStartingWith(errorMessagePrefix);

    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM %s WHERE dt >= date_add('2022-01-01', 1)", icebergTableName))
        .isInstanceOf(Exception.class)
        .hasMessageStartingWith(errorMessagePrefix);

    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM %s WHERE date '2022-01-01' = dt", icebergTableName))
        .isInstanceOf(Exception.class)
        .hasMessageStartingWith(errorMessagePrefix);

    // success cases - normal cases
    sql("SELECT * FROM %s WHERE dt = '2022-01-01'", icebergTableName);
    sql("SELECT * FROM %s WHERE dt = cast(date '2022-01-01' as string)", icebergTableName);

    // success cases - filter on non-partition
    sql("SELECT * FROM %s WHERE id = '123'", icebergTableName);
  }

  @Test
  public void testHiveTable() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    sql(
        "CREATE TABLE %s (id STRING, dt STRING) USING parquet partitioned by (dt) LOCATION '%s'",
        hiveTableName, temp.newFolder().toString());

    // Only test 1 Iceberg failure cases.
    sql("SELECT * FROM %s WHERE dt = date '2022-01-01'", hiveTableName);
  }
}
