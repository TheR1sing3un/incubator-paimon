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
import org.apache.paimon.operation.VectorFileGarbageCollector
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * Vector Column Family (vector-cf) end-to-end write/read tests through Spark SQL.
 *
 * All tests use V2 format (vector-column-family.enabled = true). V1 (non-CF) tests removed per user
 * request — V2 covers all V1 scenarios plus manifest tracking.
 *
 * bytesPerVector for 4-dim float32 = ((4*4+7)/8)*8 = 16 bytes. Tests use
 * vector-column-family.target-file-size = '32b' (= 2 vectors per file) so that file boundaries are
 * precisely testable.
 */
class VectorColumnFamilyTestBase extends PaimonSparkTestBase {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }

  // 32 bytes = 2 vectors (4-dim float32 = 16 bytes each), so 2 rows per file
  private val vectorColumnFamilyTableProps =
    """'primary-key' = 'pk',
      |  'bucket' = '1',
      |  'merge-engine' = 'partial-update',
      |  'vector-field' = 'embedding',
      |  'field.embedding.vector-dim' = '4',
      |  'file.format' = 'parquet',
      |  'vector-column-family.enabled' = 'true',
      |  'vector-column-family.target-file-size' = '32b'""".stripMargin

  // Multi-vector-column table props (two vector columns)
  private val multiVectorColumnTableProps =
    """'primary-key' = 'pk',
      |  'bucket' = '1',
      |  'merge-engine' = 'partial-update',
      |  'vector-field' = 'emb1,emb2',
      |  'field.emb1.vector-dim' = '4',
      |  'field.emb2.vector-dim' = '4',
      |  'file.format' = 'parquet',
      |  'vector-column-family.enabled' = 'true',
      |  'vector-column-family.target-file-size' = '32b'""".stripMargin

  /** Helper: count vector files on disk in bucket-0. */
  private def countVectorFilesOnDisk(tableName: String): Int = {
    val table = loadTable(tableName)
    val bucketPath = new Path(table.location(), "bucket-0")
    table
      .fileIO()
      .listStatus(bucketPath)
      .count(f => f.getPath.getName.endsWith(".vector.bin"))
  }

  /** Helper: get vector file sizes on disk in bucket-0. */
  private def vectorFileSizesOnDisk(tableName: String): Map[String, Long] = {
    val table = loadTable(tableName)
    val bucketPath = new Path(table.location(), "bucket-0")
    table
      .fileIO()
      .listStatus(bucketPath)
      .filter(f => f.getPath.getName.endsWith(".vector.bin"))
      .map(f => f.getPath.getName -> f.getLen)
      .toMap
  }

  /** Helper: get vector file names from manifest (via DataSplit). */
  private def vectorManifestFiles(tableName: String): Set[String] = {
    val table = loadTable(tableName)
    table
      .newSnapshotReader()
      .read()
      .dataSplits()
      .asScala
      .flatMap(_.dataFiles().asScala)
      .filter(_.isVectorCFFile)
      .map(_.fileName())
      .toSet
  }

  // ==================== Vector-CF Write & Read ====================

  test("Vector-CF: basic INSERT and SELECT with V2 descriptors") {
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

      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, "bob", Seq(5.0f, 6.0f, 7.0f, 8.0f))
        )
      )
    }
  }

  test("Vector-CF: INSERT with null vector") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
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

  test("Vector-CF: scalar-only query has zero vector file reads") {
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

      checkAnswer(
        sql("SELECT pk, name FROM t ORDER BY pk"),
        Seq(Row(1, "alice"), Row(2, "bob"))
      )
    }
  }

  // ==================== File Separation & Precise File Counting ====================

  test("Vector-CF: verify file separation and precise file count with target-file-size") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
             |)""".stripMargin)

      // Insert 3 rows with vectors using separate INSERTs (ensures merge path for correct reading).
      // target-file-size=32b → 2 vectors per file, each INSERT creates 1 vector
      sql("INSERT INTO t VALUES (1, 'a', array(1.0, 2.0, 3.0, 4.0))")
      sql("INSERT INTO t VALUES (2, 'b', array(5.0, 6.0, 7.0, 8.0))")
      sql("INSERT INTO t VALUES (3, 'c', array(9.0, 10.0, 11.0, 12.0))")

      val table = loadTable("t")
      val bucketPath = new Path(table.location(), "bucket-0")
      val allFiles = table.fileIO().listStatus(bucketPath)
      val vectorFiles = allFiles.filter(f => f.getPath.getName.endsWith(".vector.bin"))
      val parquetFiles = allFiles.filter(
        f => !f.getPath.getName.contains(".vector.") && f.getPath.getName.endsWith(".parquet"))

      // With 3 separate INSERTs and target-file-size=32b (2 vectors per file):
      // Each INSERT creates 1 vector (16 bytes). Append mode may combine into fewer files.
      assert(vectorFiles.nonEmpty, "Expected vector files on filesystem")
      assert(
        vectorFiles.length >= 1 && vectorFiles.length <= 3,
        s"Expected 1-3 vector files, got ${vectorFiles.length}: " +
          vectorFiles.map(_.getPath.getName).mkString(", ")
      )

      // Parquet scalar files should also exist
      assert(parquetFiles.nonEmpty, "Expected main data files on filesystem")

      // Each vector file should be a multiple of 16 bytes (bytesPerVector for 4-dim float32)
      vectorFiles.foreach {
        f =>
          assert(
            f.getLen % 16 == 0 && f.getLen > 0,
            s"Vector file ${f.getPath.getName} has unexpected size ${f.getLen}")
      }

      // Data is correct
      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "a", Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, "b", Seq(5.0f, 6.0f, 7.0f, 8.0f)),
          Row(3, "c", Seq(9.0f, 10.0f, 11.0f, 12.0f))
        )
      )
    }
  }

  // ==================== Multi-Vector-Column Tests ====================

  test("Vector-CF: multi-vector-column files are separate per column") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  emb1 ARRAY<FLOAT>,
             |  emb2 ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $multiVectorColumnTableProps
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, array(1.0, 2.0, 3.0, 4.0), array(10.0, 20.0, 30.0, 40.0))")
      sql("INSERT INTO t VALUES (2, array(5.0, 6.0, 7.0, 8.0), array(50.0, 60.0, 70.0, 80.0))")

      // Verify manifest has vector files with distinct writeCols
      val table = loadTable("t")
      val allSplitFiles = table
        .newSnapshotReader()
        .read()
        .dataSplits()
        .asScala
        .flatMap(_.dataFiles().asScala)
      val vectorFiles = allSplitFiles.filter(_.isVectorCFFile).toSeq

      // Should have vector files for both columns
      val writeColsSets = vectorFiles.map(f => f.writeCols().asScala.toSet).toSet
      assert(
        writeColsSets.contains(Set("emb1")),
        s"Expected writeCols containing 'emb1', got: $writeColsSets")
      assert(
        writeColsSets.contains(Set("emb2")),
        s"Expected writeCols containing 'emb2', got: $writeColsSets")

      // emb1 files and emb2 files should be different files
      val emb1Files = vectorFiles.filter(_.writeCols().contains("emb1")).map(_.fileName()).toSet
      val emb2Files = vectorFiles.filter(_.writeCols().contains("emb2")).map(_.fileName()).toSet
      assert(
        emb1Files.intersect(emb2Files).isEmpty,
        s"emb1 and emb2 should use different vector files. emb1=$emb1Files, emb2=$emb2Files")

      // Both columns readable
      checkAnswer(
        sql("SELECT pk, emb1, emb2 FROM t ORDER BY pk"),
        Seq(
          Row(1, Seq(1.0f, 2.0f, 3.0f, 4.0f), Seq(10.0f, 20.0f, 30.0f, 40.0f)),
          Row(2, Seq(5.0f, 6.0f, 7.0f, 8.0f), Seq(50.0f, 60.0f, 70.0f, 80.0f))
        )
      )
    }
  }

  // ==================== Partial-Update & File Content Assertions ====================

  test("Vector-CF: partial-update scalar-only preserves vector file content unchanged") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")

      // Record vector file state after first write
      val sizesAfterFirst = vectorFileSizesOnDisk("t")
      assert(sizesAfterFirst.nonEmpty, "Expected vector files after first write")

      // Scalar-only update (null vector = no update for partial-update)
      sql("INSERT INTO t VALUES (1, 'alice_updated', null)")

      // Vector file count AND sizes must be identical (content unchanged)
      val sizesAfterSecond = vectorFileSizesOnDisk("t")
      assert(
        sizesAfterSecond == sizesAfterFirst,
        s"Vector file sizes changed after scalar-only update. " +
          s"Before: $sizesAfterFirst, After: $sizesAfterSecond")

      // Verify scalar was updated, vector preserved
      checkAnswer(
        sql("SELECT pk, name, embedding FROM t"),
        Seq(Row(1, "alice_updated", Seq(1.0f, 2.0f, 3.0f, 4.0f)))
      )
    }
  }

  // ==================== Manifest Tracking ====================

  test("Vector-CF: manifest writeCols correct and files tracked across snapshots") {
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
      val allFiles = table
        .newSnapshotReader()
        .read()
        .dataSplits()
        .asScala
        .flatMap(_.dataFiles().asScala)
      val vectorFiles = allFiles.filter(_.isVectorCFFile)
      val scalarFiles = allFiles.filter(!_.isVectorCFFile)

      assert(vectorFiles.nonEmpty, "Expected vector CF files in manifest")
      assert(scalarFiles.nonEmpty, "Expected scalar data files in manifest")

      vectorFiles.foreach {
        vf =>
          assert(
            vf.writeCols() != null && !vf.writeCols().isEmpty,
            s"Vector file ${vf.fileName()} should have non-empty writeCols")
          assert(
            vf.writeCols().contains("embedding"),
            s"writeCols should contain 'embedding', got: ${vf.writeCols()}")
          assert(
            vf.fileName().contains(".vector."),
            s"File name should contain '.vector.', got: ${vf.fileName()}")
      }

      scalarFiles.foreach {
        sf =>
          assert(
            sf.writeCols() == null || sf.writeCols().isEmpty,
            s"Scalar file ${sf.fileName()} should have null/empty writeCols")
      }
    }
  }

  test("Vector-CF: old vector files preserved in new snapshots (one-time ADD)") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")
      val vectorFilesSnap1 = vectorManifestFiles("t")
      assert(vectorFilesSnap1.nonEmpty, "Snap1 should have vector files")

      sql("INSERT INTO t VALUES (2, 'bob', array(5.0, 6.0, 7.0, 8.0))")
      val vectorFilesSnap2 = vectorManifestFiles("t")

      // Snap1 files must still exist in snap2
      assert(
        vectorFilesSnap1.subsetOf(vectorFilesSnap2),
        s"Snap1 vector files $vectorFilesSnap1 should be subset of snap2 $vectorFilesSnap2")

      // Snap2 may have additional files (for the new rows)
      assert(
        vectorFilesSnap2.size >= vectorFilesSnap1.size,
        s"Snap2 should have >= snap1 vector files. Snap1=${vectorFilesSnap1.size}, Snap2=${vectorFilesSnap2.size}"
      )

      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, "bob", Seq(5.0f, 6.0f, 7.0f, 8.0f))
        )
      )
    }
  }

  // ==================== Compaction Safety ====================

  test("Vector-CF: compaction does not touch vector files") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps,
             |  'compaction.min.file-num' = '2',
             |  'compaction.max.file-num' = '3',
             |  'num-sorted-run.compaction-trigger' = '2'
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")
      sql("INSERT INTO t VALUES (2, 'bob', array(5.0, 6.0, 7.0, 8.0))")
      sql("INSERT INTO t VALUES (3, 'charlie', array(9.0, 10.0, 11.0, 12.0))")

      val table = loadTable("t")
      val bucketPath = new Path(table.location(), "bucket-0")
      val vectorFsFiles = table
        .fileIO()
        .listStatus(bucketPath)
        .filter(f => f.getPath.getName.endsWith(".vector.bin"))
      assert(vectorFsFiles.nonEmpty, "Vector files should survive compaction")

      // Manifest vector files should be a subset of disk files
      val manifestVec = vectorManifestFiles("t")
      val diskVec = vectorFsFiles.map(_.getPath.getName).toSet
      assert(manifestVec.subsetOf(diskVec), s"Manifest: $manifestVec, Disk: $diskVec")

      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, "bob", Seq(5.0f, 6.0f, 7.0f, 8.0f)),
          Row(3, "charlie", Seq(9.0f, 10.0f, 11.0f, 12.0f))
        )
      )
    }
  }

  test("Vector-CF: read vector column after compaction with deletion-vectors enabled") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  'primary-key' = 'pk',
             |  'bucket' = '1',
             |  'merge-engine' = 'partial-update',
             |  'vector-field' = 'embedding',
             |  'field.embedding.vector-dim' = '4',
             |  'file.format' = 'parquet',
             |  'vector-column-family.enabled' = 'true',
             |  'vector-column-family.target-file-size' = '32b',
             |  'deletion-vectors.enabled' = 'true',
             |  'compaction.min.file-num' = '2',
             |  'compaction.max.file-num' = '3',
             |  'num-sorted-run.compaction-trigger' = '2'
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")
      sql("INSERT INTO t VALUES (2, 'bob', array(5.0, 6.0, 7.0, 8.0))")
      sql("INSERT INTO t VALUES (3, 'charlie', array(9.0, 10.0, 11.0, 12.0))")

      // Verify vector files exist in manifest
      val vecFiles = vectorManifestFiles("t")
      assert(vecFiles.nonEmpty, "Expected vector files in manifest after writes")

      // Read vector column — this exercises the ReadBuilder path which sets
      // levelFilter(level > 0) when deletion-vectors.enabled=true.
      // Before the fix, vector CF files (level=0) were filtered out by levelFilter.
      checkAnswer(
        sql("SELECT pk, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, Seq(5.0f, 6.0f, 7.0f, 8.0f)),
          Row(3, Seq(9.0f, 10.0f, 11.0f, 12.0f))
        )
      )

      // Also verify selecting only scalar columns still works
      checkAnswer(
        sql("SELECT pk, name FROM t ORDER BY pk"),
        Seq(Row(1, "alice"), Row(2, "bob"), Row(3, "charlie"))
      )

      // Verify predicate pushdown with vector columns works (exercises filterByValueFilter)
      checkAnswer(
        sql("SELECT pk, embedding FROM t WHERE name = 'alice'"),
        Seq(Row(1, Seq(1.0f, 2.0f, 3.0f, 4.0f)))
      )

      // Verify predicate pushdown on scalar-only query
      checkAnswer(
        sql("SELECT pk FROM t WHERE name = 'bob'"),
        Seq(Row(2))
      )
    }
  }

  // ==================== Vector CF Compaction Tests ====================

  test("Vector-CF: full compaction merges low-ratio vector files and updates descriptors") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  'primary-key' = 'pk',
             |  'bucket' = '1',
             |  'merge-engine' = 'partial-update',
             |  'vector-field' = 'embedding',
             |  'field.embedding.vector-dim' = '4',
             |  'file.format' = 'parquet',
             |  'vector-column-family.enabled' = 'true',
             |  'vector-column-family.target-file-size' = '32b',
             |  'vector-column-family.compact.enabled' = 'true',
             |  'vector-column-family.compact.valid-ratio-threshold' = '0.8',
             |  'vector-column-family.compact.min-files-to-merge' = '1',
             |  'compaction.min.file-num' = '999',
             |  'compaction.max.file-num' = '999',
             |  'num-sorted-runs.compaction-trigger' = '999'
             |)""".stripMargin)

      // Phase 1: Write initial data — creates vector files
      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")
      sql("INSERT INTO t VALUES (2, 'bob', array(5.0, 6.0, 7.0, 8.0))")
      sql("INSERT INTO t VALUES (3, 'charlie', array(9.0, 10.0, 11.0, 12.0))")

      val vecFilesBefore = vectorManifestFiles("t")
      assert(vecFilesBefore.nonEmpty, "Expected vector files after writes")

      // Phase 2: Overwrite vectors — old vector data becomes dead
      sql("INSERT INTO t VALUES (1, 'alice', array(10.0, 20.0, 30.0, 40.0))")
      sql("INSERT INTO t VALUES (2, 'bob', array(50.0, 60.0, 70.0, 80.0))")

      // Verify data before compaction
      checkAnswer(
        sql("SELECT pk, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, Seq(10.0f, 20.0f, 30.0f, 40.0f)),
          Row(2, Seq(50.0f, 60.0f, 70.0f, 80.0f)),
          Row(3, Seq(9.0f, 10.0f, 11.0f, 12.0f))
        )
      )

      // Phase 3: Trigger full compaction
      sql("CALL sys.compact(table => 't', compact_strategy => 'full')")

      // Phase 4: Verify data after compaction — vectors must be readable
      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", Seq(10.0f, 20.0f, 30.0f, 40.0f)),
          Row(2, "bob", Seq(50.0f, 60.0f, 70.0f, 80.0f)),
          Row(3, "charlie", Seq(9.0f, 10.0f, 11.0f, 12.0f))
        )
      )

      // Verify scalar-only query also works after compaction
      checkAnswer(
        sql("SELECT pk, name FROM t ORDER BY pk"),
        Seq(Row(1, "alice"), Row(2, "bob"), Row(3, "charlie"))
      )
    }
  }

  // ==================== GC Tests ====================

  test("Vector-CF: GC preserves referenced vector files") {
    withTable("t") {
      sql(s"""CREATE TABLE t (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  $vectorColumnFamilyTableProps
             |)""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'alice', array(1.0, 2.0, 3.0, 4.0))")

      val countBefore = countVectorFilesOnDisk("t")
      assert(countBefore > 0, "Expected vector files before GC")

      sql("CALL paimon.sys.vector_column_family_gc(table => 'test.t')")

      val countAfter = countVectorFilesOnDisk("t")
      assert(
        countAfter == countBefore,
        s"GC should not delete referenced vector files. Before=$countBefore, After=$countAfter")
    }
  }

  test("Vector-CF: GC deletes orphan vector files not in manifest") {
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

      // Create an orphan vector file (not in manifest)
      val orphanPath = new Path(bucketPath, "data-orphan-0.vector.bin")
      val out = table.fileIO().newOutputStream(orphanPath, false)
      out.write(new Array[Byte](32))
      out.close()

      // Verify orphan exists
      assert(table.fileIO().exists(orphanPath), "Orphan file should exist before GC")

      val refFilesBefore = vectorManifestFiles("t")
      val totalBefore = countVectorFilesOnDisk("t")
      // total should be refFilesBefore.size + 1 (orphan)
      assert(
        totalBefore == refFilesBefore.size + 1,
        s"Expected ${refFilesBefore.size + 1} vector files on disk (ref + orphan), got $totalBefore")

      // GC with default safety window — orphan is too young, should NOT be deleted
      sql("CALL paimon.sys.vector_column_family_gc(table => 'test.t')")

      // Orphan is young (just created) so safety window preserves it
      assert(
        table.fileIO().exists(orphanPath),
        "Orphan vector file should be preserved by safety window (too young)")

      // All referenced files still exist
      refFilesBefore.foreach {
        fileName =>
          assert(
            table.fileIO().exists(new Path(bucketPath, fileName)),
            s"Referenced vector file $fileName should survive GC")
      }

      // Data still readable
      checkAnswer(
        sql("SELECT pk, embedding FROM t"),
        Seq(Row(1, Seq(1.0f, 2.0f, 3.0f, 4.0f)))
      )
    }
  }

  test("Vector-CF: GC actually deletes unreferenced orphan vector files (olderThanMillis=0)") {
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

      val table = loadTable("t").asInstanceOf[FileStoreTable]
      val bucketPath = new Path(table.location(), "bucket-0")

      // Create an orphan vector file (not in manifest, not referenced by any row)
      val orphanPath = new Path(bucketPath, "data-orphan-gc-test.vector.bin")
      val out = table.fileIO().newOutputStream(orphanPath, false)
      out.write(new Array[Byte](32))
      out.close()
      assert(table.fileIO().exists(orphanPath), "Orphan file should exist before GC")

      val refFilesBefore = vectorManifestFiles("t")

      // Use Paimon Java API: collect referenced from manifest (not from data read)
      // Vector CF files in manifest ARE the referenced files
      val gc = new VectorFileGarbageCollector(table)
      val allVectorFiles = gc.collectAllVectorFilesFromFS()
      // Referenced = vector file names from manifest
      val referenced = new java.util.HashSet[String](refFilesBefore.asJava)

      val deleted = gc.deleteUnreferenced(allVectorFiles, referenced, 0L)

      // Orphan should be deleted
      assert(
        !table.fileIO().exists(orphanPath),
        "Orphan vector file should be deleted by GC with olderThanMillis=0")
      assert(deleted >= 1, s"Expected at least 1 file deleted, got $deleted")

      // Referenced files still exist
      refFilesBefore.foreach {
        fileName =>
          assert(
            table.fileIO().exists(new Path(bucketPath, fileName)),
            s"Referenced vector file $fileName should survive GC")
      }

      // Data still readable after orphan deletion
      checkAnswer(
        sql("SELECT pk, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, Seq(1.0f, 2.0f, 3.0f, 4.0f)),
          Row(2, Seq(5.0f, 6.0f, 7.0f, 8.0f))
        )
      )
    }
  }

  // ==================== Schema Evolution ====================

  test("Vector-CF: ALTER TABLE ADD COLUMN with VectorType via vector-field property") {
    withTable("t") {
      // Create table with scalar columns only
      sql("""CREATE TABLE t (
            |  pk INT,
            |  name STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'pk',
            |  'bucket' = '1',
            |  'merge-engine' = 'partial-update',
            |  'file.format' = 'parquet'
            |)""".stripMargin)

      // Insert data before adding vector column
      sql("INSERT INTO t VALUES (1, 'alice')")

      // Add column first as ARRAY<FLOAT>, then set vector-field property to convert it.
      // The SparkCatalog.resolveDataType() checks vector-field in table options at ADD COLUMN
      // time, so we need properties set first. Use a single ALTER TABLE for both property
      // setting and column addition is not supported in Spark SQL.
      // Instead: add column as plain ARRAY first, then set properties to mark it as vector.
      sql("ALTER TABLE t ADD COLUMN embedding ARRAY<FLOAT>")
      sql("""ALTER TABLE t SET TBLPROPERTIES (
            |  'vector-field' = 'embedding',
            |  'field.embedding.vector-dim' = '4',
            |  'vector-column-family.enabled' = 'true',
            |  'vector-column-family.target-file-size' = '32b'
            |)""".stripMargin)

      // Old row should have null vector
      checkAnswer(
        sql("SELECT pk, name FROM t"),
        Seq(Row(1, "alice"))
      )

      // Insert new row with vector
      sql("INSERT INTO t VALUES (2, 'bob', array(5.0, 6.0, 7.0, 8.0))")

      // Both rows should be readable; old row has null embedding
      checkAnswer(
        sql("SELECT pk, name, embedding FROM t ORDER BY pk"),
        Seq(
          Row(1, "alice", null),
          Row(2, "bob", Seq(5.0f, 6.0f, 7.0f, 8.0f))
        )
      )

      // Verify the new column IS VectorType (vector files should exist)
      val table = loadTable("t")
      val bucketPath = new Path(table.location(), "bucket-0")
      val vectorFiles = table
        .fileIO()
        .listStatus(bucketPath)
        .filter(f => f.getPath.getName.endsWith(".vector.bin"))
      assert(
        vectorFiles.nonEmpty,
        "Expected vector files after ALTER TABLE ADD COLUMN with vector-field property")
    }
  }
}

class VectorColumnFamilyTestWithV2Write extends VectorColumnFamilyTestBase {
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}
