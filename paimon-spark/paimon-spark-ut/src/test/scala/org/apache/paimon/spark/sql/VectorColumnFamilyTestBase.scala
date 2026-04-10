/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

/**
 * IT tests for:
 *   1. Spark VectorType read/write support (basic vector through Spark SQL)
 *   2. Vector Column Family (vector-cf) end-to-end write/read through Spark SQL
 */
class VectorColumnFamilyTestBase extends PaimonSparkTestBase {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }

  private val vectorColumnFamilyTableProps =
    """'primary-key' = 'pk',
      |  'bucket' = '1',
      |  'merge-engine' = 'partial-update',
      |  'vector-field' = 'embedding',
      |  'field.embedding.vector-dim' = '4',
      |  'file.format' = 'parquet',
      |  'vector-column-family.enabled' = 'true'""".stripMargin

  // ==================== Part 1: Spark VectorType Read/Write ====================

  test("VectorType: basic INSERT and SELECT through Spark SQL") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  pk INT,
            |  name STRING,
            |  embedding ARRAY<FLOAT>
            |) TBLPROPERTIES (
            |  'primary-key' = 'pk',
            |  'bucket' = '1',
            |  'vector-field' = 'embedding',
            |  'field.embedding.vector-dim' = '4',
            |  'file.format' = 'avro'
            |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")
      sql("INSERT INTO t VALUES (2, 'bob', array(5.0, 6.0, 7.0, 8.0))")

      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, "bob", Seq(5.0f, 6.0f, 7.0f, 8.0f))
        )
      )
    }
  }

  test("VectorType: INSERT with null vector") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  pk INT,
            |  name STRING,
            |  embedding ARRAY<FLOAT>
            |) TBLPROPERTIES (
            |  'primary-key' = 'pk',
            |  'bucket' = '1',
            |  'vector-field' = 'embedding',
            |  'field.embedding.vector-dim' = '4',
            |  'file.format' = 'avro'
            |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")
      sql("INSERT INTO t VALUES (2, 'bob', null)")

      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, "bob", null)
        )
      )
    }
  }

  test("VectorType: parquet main format with vector.file.format for separate storage") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  pk INT,
            |  name STRING,
            |  embedding ARRAY<FLOAT>
            |) TBLPROPERTIES (
            |  'vector-field' = 'embedding',
            |  'field.embedding.vector-dim' = '4',
            |  'file.format' = 'parquet',
            |  'data-evolution.enabled' = 'true',
            |  'row-tracking.enabled' = 'true',
            |  'vector.file.format' = 'avro'
            |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")

      checkAnswer(
        sql("SELECT pk, name, embedding FROM t"),
        Seq(Row(1, "alice", Seq(1.0f, 2.0f, 3.0f, 4.0f)))
      )
    }
  }

  // ==================== Part 2: Vector-CF End-to-End ====================

  test("Vector column family: INSERT and verify vector file separation") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")
      sql("INSERT INTO t VALUES (2, 'bob', array(5.0, 6.0, 7.0, 8.0))")

      // Verify vector files exist on filesystem (they have ".vector." in the name)
      val table = loadTable("t")
      val bucketPath = new Path(table.location(), "bucket-0")
      val files = table.fileIO().listStatus(bucketPath)
      val vectorFiles = files.filter(f => f.getPath.getName.contains(".vector."))
      assert(vectorFiles.nonEmpty, "Expected vector column family files on filesystem")

      // Verify main data files also exist (non-vector)
      val dataFiles = files.filter(
        f => !f.getPath.getName.contains(".vector.") && f.getPath.getName.endsWith(".parquet"))
      assert(dataFiles.nonEmpty, "Expected main data files on filesystem")

      // Verify scalar columns are readable
      checkAnswer(
        sql("SELECT pk, name FROM t ORDER BY pk"),
        Seq(Row(1, "alice"), Row(2, "bob"))
      )

      // Verify vector columns are readable (descriptor dereference through read path)
      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, "bob", Seq(5.0f, 6.0f, 7.0f, 8.0f))
        )
      )
    }
  }

  test("Vector column family: partial-update scalar-only does not create new vector files") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
             |)""".stripMargin)

      // First write: rows with vector data
      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")

      val table = loadTable("t")
      val bucketPath = new Path(table.location(), "bucket-0")
      val vectorFilesAfterFirst = table
        .fileIO()
        .listStatus(bucketPath)
        .count(f => f.getPath.getName.contains(".vector."))

      // Second write: scalar-only update (null vector = no update for partial-update)
      sql("INSERT INTO t VALUES (1, 'alice_updated', null)")

      val vectorFilesAfterSecond = loadTable("t")
        .fileIO()
        .listStatus(bucketPath)
        .count(f => f.getPath.getName.contains(".vector."))

      // No new vector files should be created for scalar-only updates
      assert(
        vectorFilesAfterSecond == vectorFilesAfterFirst,
        s"Expected no new vector files for scalar-only update, " +
          s"but got $vectorFilesAfterSecond vs $vectorFilesAfterFirst"
      )

      // Verify scalar column was updated
      checkAnswer(
        sql("SELECT pk, name FROM t"),
        Seq(Row(1, "alice_updated"))
      )

      // Verify vector data is preserved after scalar-only partial update
      checkAnswer(
        sql("SELECT pk, name, embedding FROM t"),
        Seq(Row(1, "alice_updated", Seq(1.0f, 2.0f, 3.0f, 4.0f)))
      )
    }
  }

  test("Vector column family: GC procedure keeps referenced vector files") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")

      val table = loadTable("t")
      val bucketPath = new Path(table.location(), "bucket-0")
      val vectorFilesBefore = table
        .fileIO()
        .listStatus(bucketPath)
        .filter(f => f.getPath.getName.contains(".vector."))
      assert(vectorFilesBefore.nonEmpty, "Expected vector files to exist before GC")

      // Call GC procedure — since all vector files are referenced, none should be deleted
      val gcResult = sql("CALL paimon.sys.vector_column_family_gc(table => 'test.t')").collect()
      assert(gcResult.nonEmpty)

      val vectorFilesAfter = table
        .fileIO()
        .listStatus(bucketPath)
        .filter(f => f.getPath.getName.contains(".vector."))
      assert(
        vectorFilesAfter.length == vectorFilesBefore.length,
        "GC should not delete referenced vector files"
      )
    }
  }
}

class VectorColumnFamilyTestWithV2Write extends VectorColumnFamilyTestBase {
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}
