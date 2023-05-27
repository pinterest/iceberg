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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestPinterestCopyOnWriteDelete extends SparkRowLevelOperationsTestBase {
  boolean fileAsSplit = false;

  public TestPinterestCopyOnWriteDelete(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      Boolean vectorized,
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @Parameterized.Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2},"
              + " format = {3}, vectorized = {4}, distributionMode = {5}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            "clients",
            "1",
            "parquet-enabled",
            "true",
            "cache-enabled",
            "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        "parquet",
        false,
        TableProperties.WRITE_DISTRIBUTION_MODE_RANGE
      }
    };
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS deleted_id");
    sql("DROP TABLE IF EXISTS deleted_dep");
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.DELETE_MODE,
        RowLevelOperationMode.COPY_ON_WRITE.modeName(),
        TableProperties.DELETE_DISTRIBUTION_MODE,
        DistributionMode.RANGE.modeName(),
        TableProperties.FILE_AS_SPLIT,
        String.valueOf(fileAsSplit));
  }

  /**
   * Checks if there are unordered files, deleting a row using CoW will use shuffle and order the
   * files for range partitioning when sort optimization for CoW is turned off through fileAsSplit.
   */
  @Test
  public void testUnorderedFilesWithoutOptimization() throws NoSuchTableException {
    unorderedFilesTest(false);
  }

  /**
   * Checks if there are unordered files, deleting a row using CoW will use shuffle and order the
   * files for range partitioning, even if sort optimization for CoW is turned on through
   * fileAsSplit.
   */
  @Test
  public void testUnorderedFilesWithOptimization() throws NoSuchTableException {
    unorderedFilesTest(true);
  }

  /**
   * Checks if there are ordered files, deleting a row using CoW will use shuffle and order the
   * files for range partitioning, even if sort optimization for CoW is turned on through
   * fileAsSplit.
   */
  @Test
  public void testOrderedFilesWithoutOptimization() throws NoSuchTableException {
    orderedFilesTest(false);
  }

  /**
   * Checks if there are ordered files, deleting a row using CoW will not use shuffle and maintain
   * existing order in files when sort optimization for CoW is turned on through fileAsSplit.
   */
  @Test
  public void testOrderedFilesWithOptimization() throws NoSuchTableException {
    orderedFilesTest(true);
  }

  /**
   * Checks if there are ordered files, deleting a row using CoW will not use shuffle and maintain
   * existing order in files when sort optimization for CoW is turned on through fileAsSplit, even
   * when scan includes multiple partitions.
   */
  @Test
  public void testOrderedFilesMultiPartitions() throws NoSuchTableException {
    this.fileAsSplit = true;
    createAndInitPartitionedTable();

    // enable write ordering to create sorted files
    sql("ALTER TABLE %s WRITE ORDERED BY %s", tableName, "id");

    // set shuffle partitions to 1 to create required files
    spark.conf().set("spark.sql.shuffle.partitions", "1");

    // create unsorted files
    append(new Employee(3, "hr"), new Employee(1, "hr"));
    append(new Employee(2, "hardware"), new Employee(5, "hardware"), new Employee(1, "hardware"));
    append(new Employee(2, "hardware"), new Employee(3, "hardware"), new Employee(4, "hardware"));

    // check files are created as expected
    ImmutableList<ImmutableList<Serializable>> expectedCountBoundsAndSortIds =
        ImmutableList.of(
            ImmutableList.of(
                3L,
                1,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 2).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 4).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array())),
            ImmutableList.of(
                3L,
                1,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 1).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 5).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array())),
            ImmutableList.of(
                2L,
                1,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 1).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hr").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 3).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hr").array())));
    validateCountBoundsAndSortIds(expectedCountBoundsAndSortIds);

    // set shuffle partitions to 4 to induce shuffling when possible
    spark.conf().set("spark.sql.shuffle.partitions", "4");
    sql("DELETE FROM %s WHERE id = 1", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));
    Snapshot currentSnapshot = table.currentSnapshot();

    // validate that more files are added than the deleted files
    validateCopyOnWrite(currentSnapshot, "2", "2", "2");
  }

  private void unorderedFilesTest(boolean processOneFilePerTask) throws NoSuchTableException {
    this.fileAsSplit = processOneFilePerTask;
    createAndInitPartitionedTable();

    // set shuffle partitions to 1 to create required files
    spark.conf().set("spark.sql.shuffle.partitions", "1");

    // create unsorted files
    append(new Employee(3, "hr"), new Employee(1, "hr"));
    append(new Employee(2, "hardware"), new Employee(5, "hardware"), new Employee(1, "hardware"));

    // check files are created as expected
    ImmutableList<ImmutableList<Serializable>> expectedCountBoundsAndSortIds =
        ImmutableList.of(
            ImmutableList.of(
                3L,
                0,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 1).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 5).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array())),
            ImmutableList.of(
                2L,
                0,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 1).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hr").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 3).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hr").array())));
    validateCountBoundsAndSortIds(expectedCountBoundsAndSortIds);

    // enable write ordering to create sorted files
    sql("ALTER TABLE %s WRITE ORDERED BY %s", tableName, "id");

    // create sorted files
    append(new Employee(2, "hardware"), new Employee(3, "hardware"), new Employee(4, "hardware"));

    validateCountBoundsAndSortIds(
        ImmutableList.<ImmutableList<Serializable>>builder()
            .add(
                ImmutableList.of(
                    3L,
                    1,
                    ImmutableList.of(
                        Conversions.toByteBuffer(Type.TypeID.INTEGER, 2).array(),
                        Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array()),
                    ImmutableList.of(
                        Conversions.toByteBuffer(Type.TypeID.INTEGER, 4).array(),
                        Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array())))
            .addAll(expectedCountBoundsAndSortIds)
            .build());

    // set shuffle partitions to 4 to induce shuffling when possible
    spark.conf().set("spark.sql.shuffle.partitions", "4");
    sql("DELETE FROM %s WHERE id = 2", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));
    Snapshot currentSnapshot = table.currentSnapshot();

    // validate that more files are added than the deleted files
    validateCopyOnWrite(currentSnapshot, "1", "2", "4");
  }

  private void orderedFilesTest(boolean processOneFilePerTask) throws NoSuchTableException {
    this.fileAsSplit = processOneFilePerTask;
    createAndInitPartitionedTable();

    // enable write ordering to create sorted files
    sql("ALTER TABLE %s WRITE ORDERED BY %s", tableName, "id");

    // set shuffle partitions to 1 to create required files
    spark.conf().set("spark.sql.shuffle.partitions", "1");

    // create unsorted files
    append(new Employee(3, "hr"), new Employee(1, "hr"));
    append(new Employee(2, "hardware"), new Employee(5, "hardware"), new Employee(1, "hardware"));
    append(new Employee(2, "hardware"), new Employee(3, "hardware"), new Employee(4, "hardware"));

    // check files are created as expected
    ImmutableList<ImmutableList<Serializable>> expectedCountBoundsAndSortIds =
        ImmutableList.of(
            ImmutableList.of(
                3L,
                1,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 2).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 4).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array())),
            ImmutableList.of(
                3L,
                1,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 1).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 5).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hardware").array())),
            ImmutableList.of(
                2L,
                1,
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 1).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hr").array()),
                ImmutableList.of(
                    Conversions.toByteBuffer(Type.TypeID.INTEGER, 3).array(),
                    Conversions.toByteBuffer(Type.TypeID.STRING, "hr").array())));
    validateCountBoundsAndSortIds(expectedCountBoundsAndSortIds);

    // set shuffle partitions to 4 to induce shuffling when possible
    spark.conf().set("spark.sql.shuffle.partitions", "4");
    sql("DELETE FROM %s WHERE id = 2", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));
    Snapshot currentSnapshot = table.currentSnapshot();

    // validate that more files are added than the deleted files
    validateCopyOnWrite(currentSnapshot, "1", "2", processOneFilePerTask ? "2" : "4");
  }

  private void validateCountBoundsAndSortIds(
      ImmutableList<ImmutableList<Serializable>> expectedCountBoundsAndSortIds) {
    List<Object[]> countBoundsAndSortIds =
        sql(
            "select %s, %s, %s, %s from %s.files",
            DataFile.RECORD_COUNT.name(),
            DataFile.SORT_ORDER_ID.name(),
            DataFile.LOWER_BOUNDS.name(),
            DataFile.UPPER_BOUNDS.name(),
            tableName);

    Assert.assertEquals(
        "Number of files mismatch",
        expectedCountBoundsAndSortIds.size(),
        countBoundsAndSortIds.size());
    int currentRow = 0;
    for (ImmutableList<Serializable> expectedCountBoundsAndSortId : expectedCountBoundsAndSortIds) {
      long recordCount = (Long) expectedCountBoundsAndSortId.get(0);
      int sortId = (int) expectedCountBoundsAndSortId.get(1);
      List<Object> lowerBounds = (List<Object>) expectedCountBoundsAndSortId.get(2);
      List<Object> upperBounds = (List<Object>) expectedCountBoundsAndSortId.get(3);

      Assert.assertEquals(recordCount, (long) countBoundsAndSortIds.get(currentRow)[0]);
      Assert.assertEquals(sortId, (int) countBoundsAndSortIds.get(currentRow)[1]);
      Assert.assertArrayEquals(
          lowerBounds.toArray(),
          ((Map<Integer, byte[]>) countBoundsAndSortIds.get(currentRow)[2]).values().toArray());
      Assert.assertArrayEquals(
          upperBounds.toArray(),
          ((Map<Integer, byte[]>) countBoundsAndSortIds.get(currentRow)[3]).values().toArray());
      ++currentRow;
    }
  }

  private void createAndInitPartitionedTable() {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tableName);
    initTable();
  }

  private void append(Employee... employees) throws NoSuchTableException {
    List<Employee> input = Arrays.asList(employees);
    Dataset<Row> inputDF = spark.createDataFrame(input, Employee.class);
    inputDF.coalesce(1).writeTo(tableName).append();
  }
}
