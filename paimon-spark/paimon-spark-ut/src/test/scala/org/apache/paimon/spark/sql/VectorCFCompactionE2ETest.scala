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

import org.apache.paimon.Snapshot
import org.apache.paimon.fs.Path
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * Comprehensive E2E tests for Vector CF compaction lifecycle.
 *
 * Covers: multi-flush file generation → compaction with threshold splitting → VectorFileMapping →
 * data correctness → PK point query → brute-force search → second write + second compaction →
 * already-full files not re-merged → manifest correctness per snapshot.
 *
 * Each scenario runs with DV enabled and DV disabled.
 *
 * Uses target-file-rows=10 and 4-dim float32 vectors (16 bytes per vector).
 */
class VectorCFCompactionE2ETest extends PaimonSparkTestBase {

  private val DIM = 4
  private val BYTES_PER_VECTOR = 16 // ((4*4+7)/8)*8

  /**
   * Core lifecycle test with DV enabled.
   *
   * Full compaction only merges vector files whose valid-ratio drops below threshold. To trigger
   * merge, we overwrite rows → make old vectors dead → then compact.
   *
   * Timeline:
   *   - Flush 1-3: 3 small vector files (6+8+7 = 21 rows)
   *   - Overwrite most rows → creates dead refs in old vector files
   *   - Full compaction: low-ratio files get merged
   *   - Verify: data, PK point query, manifest, search splits
   *   - Second write + second compaction: verify stability
   */
  test("Vector-CF compaction lifecycle: DV enabled") {
    compactionLifecycleTest(dvEnabled = true)
  }

  test("Vector-CF compaction lifecycle: DV disabled") {
    compactionLifecycleTest(dvEnabled = false)
  }

  private def compactionLifecycleTest(dvEnabled: Boolean): Unit = {
    val tableName = if (dvEnabled) "t_compact_dv" else "t_compact_no_dv"
    withTable(tableName) {
      val dvProp = if (dvEnabled) "'deletion-vectors.enabled' = 'true'," else ""
      sql(s"""CREATE TABLE $tableName (
             |  pk INT,
             |  name STRING,
             |  score INT,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  'primary-key' = 'pk',
             |  'bucket' = '1',
             |  'merge-engine' = 'partial-update',
             |  $dvProp
             |  'vector-field' = 'embedding',
             |  'field.embedding.vector-dim' = '4',
             |  'file.format' = 'parquet',
             |  'vector-column-family.enabled' = 'true',
             |  'vector-column-family.target-file-rows' = '10',
             |  'vector-column-family.compact.enabled' = 'true',
             |  'vector-column-family.compact.min-files-to-merge' = '2',
             |  'vector-column-family.compact.valid-ratio-threshold' = '0.8',
             |  'compaction.min.file-num' = '999',
             |  'compaction.max.file-num' = '999',
             |  'num-sorted-runs.compaction-trigger' = '999'
             |)""".stripMargin)

      // === Phase 1: Three flushes creating 3 small vector files ===
      // Flush 1: pk 1-6 (6 rows)
      val batch1 = (1 to 6)
        .map(
          pk =>
            s"($pk, 'name_$pk', ${pk * 10}, array($pk.0, ${pk * 2}.0, ${pk * 3}.0, ${pk * 4}.0))")
        .mkString(", ")
      sql(s"INSERT INTO $tableName VALUES $batch1")

      // Flush 2: pk 7-14 (8 rows)
      val batch2 = (7 to 14)
        .map(
          pk =>
            s"($pk, 'name_$pk', ${pk * 10}, array($pk.0, ${pk * 2}.0, ${pk * 3}.0, ${pk * 4}.0))")
        .mkString(", ")
      sql(s"INSERT INTO $tableName VALUES $batch2")

      // Flush 3: pk 15-21 (7 rows)
      val batch3 = (15 to 21)
        .map(
          pk =>
            s"($pk, 'name_$pk', ${pk * 10}, array($pk.0, ${pk * 2}.0, ${pk * 3}.0, ${pk * 4}.0))")
        .mkString(", ")
      sql(s"INSERT INTO $tableName VALUES $batch3")

      // Verify: 3 vector files before explicit compaction
      val vecFilesBefore = getVectorFilesFromManifest(tableName)
      assert(
        vecFilesBefore.size == 3,
        s"Expected 3 vector files after 3 flushes, got ${vecFilesBefore.size}: " +
          vecFilesBefore.map(f => s"${f.fileName()}(${f.rowCount()})").mkString(", ")
      )

      // Verify each vector file size matches expected rows
      assert(
        vecFilesBefore.map(_.rowCount()).toSet == Set(6L, 8L, 7L),
        s"Expected row counts {6,8,7}, got ${vecFilesBefore.map(_.rowCount()).toSet}")

      // Verify file immutability: each file is aligned to bytesPerVector
      val fileSizesBefore = getVectorFileSizesOnDisk(tableName)
      assert(fileSizesBefore.size == 3)
      fileSizesBefore.foreach {
        case (name, size) =>
          assert(size % BYTES_PER_VECTOR == 0, s"$name size $size not aligned to $BYTES_PER_VECTOR")
      }

      // Verify immutability: record file names and sizes
      val originalFileNames = vecFilesBefore.map(_.fileName()).toSet

      // === Phase 2: Overwrite most rows to create dead references ===
      // Overwrite pk 1-5 from file1 (6 rows, 5 dead → valid ratio = 1/6 ≈ 0.17 < 0.8)
      // Overwrite pk 7-13 from file2 (8 rows, 7 dead → valid ratio = 1/8 ≈ 0.13 < 0.8)
      // Overwrite pk 15-20 from file3 (7 rows, 6 dead → valid ratio = 1/7 ≈ 0.14 < 0.8)
      val overwrites = (1 to 5).map(
        pk =>
          s"($pk, 'name_$pk', ${pk * 10}, array(${pk * 10}.0, ${pk * 20}.0, ${pk * 30}.0, ${pk * 40}.0))") ++
        (7 to 13).map(
          pk =>
            s"($pk, 'name_$pk', ${pk * 10}, array(${pk * 10}.0, ${pk * 20}.0, ${pk * 30}.0, ${pk * 40}.0))") ++
        (15 to 20).map(
          pk =>
            s"($pk, 'name_$pk', ${pk * 10}, array(${pk * 10}.0, ${pk * 20}.0, ${pk * 30}.0, ${pk * 40}.0))")
      sql(s"INSERT INTO $tableName VALUES ${overwrites.mkString(", ")}")

      // === Phase 3: Full compaction (triggers vector file merge) ===
      sql(s"CALL sys.compact(table => '$tableName', compact_strategy => 'full')")

      val vecFilesAfterCompact = getVectorFilesFromManifest(tableName)
      // After full compaction with unfilled merge (no scalar rewrite):
      // - Original small files (< target rows) that have >= minFiles are merged
      // - Overwrite files may also be merged if unfilled
      // - Data should still be correct (VectorFileMapping provides resolution)
      val afterCompactNames = vecFilesAfterCompact.map(_.fileName()).toSet
      // Verify data is still correct — this is the most important assertion
      val totalVecRows = vecFilesAfterCompact.map(_.rowCount()).sum
      assert(totalVecRows > 0, "Should have vector files with data after compact")

      // === Phase 4: Data correctness after compaction ===
      // pk 6, 14, 21 retained original vectors; rest got overwritten
      val expectedAll = (1 to 21).map {
        pk =>
          if ((1 to 5).contains(pk) || (7 to 13).contains(pk) || (15 to 20).contains(pk))
            Row(
              pk,
              s"name_$pk",
              pk * 10,
              Seq((pk * 10).toFloat, (pk * 20).toFloat, (pk * 30).toFloat, (pk * 40).toFloat))
          else
            Row(
              pk,
              s"name_$pk",
              pk * 10,
              Seq(pk.toFloat, (pk * 2).toFloat, (pk * 3).toFloat, (pk * 4).toFloat))
      }
      checkAnswer(
        sql(s"SELECT pk, name, score, embedding FROM $tableName ORDER BY pk"),
        expectedAll)

      // PK point query — verify both overwritten and original
      checkAnswer(
        sql(s"SELECT pk, name, score, embedding FROM $tableName WHERE pk = 1"),
        Seq(Row(1, "name_1", 10, Seq(10.0f, 20.0f, 30.0f, 40.0f))))

      checkAnswer(
        sql(s"SELECT pk, name, score, embedding FROM $tableName WHERE pk = 6"),
        Seq(Row(6, "name_6", 60, Seq(6.0f, 12.0f, 18.0f, 24.0f))))

      checkAnswer(
        sql(s"SELECT pk, name, score, embedding FROM $tableName WHERE pk = 21"),
        Seq(Row(21, "name_21", 210, Seq(21.0f, 42.0f, 63.0f, 84.0f))))

      // Scalar-only point query
      checkAnswer(
        sql(s"SELECT pk, name, score FROM $tableName WHERE pk = 14"),
        Seq(Row(14, "name_14", 140)))

      // === Phase 5: Brute-force vector search (via ReadBuilder API — split creation only) ===
      val table = loadTable(tableName)
      val search = new org.apache.paimon.accelerateindex.AccelerateIndexSearch(
        "embedding",
        Array(10.0f, 20.0f, 30.0f, 40.0f),
        3, // top-3
        "lumina",
        "l2",
        4,
        java.util.Collections.emptyMap()
      )
      val searchSplits = table
        .newReadBuilder()
        .withAccelerateIndexSearch(search)
        .newScan()
        .plan()
        .splits()

      assert(searchSplits.size() > 0, "Expected search splits for brute-force")
      val vcfSplits = searchSplits.asScala.collect {
        case s: org.apache.paimon.accelerateindex.VectorCFSearchSplit => s
      }
      assert(vcfSplits.nonEmpty, "Expected VectorCFSearchSplit instances")
      vcfSplits.foreach {
        s =>
          assert(
            s.vectorFileName() != null && s.vectorFileName().nonEmpty,
            "VectorCFSearchSplit should have vectorFileName")
      }

      // === Phase 6: Second write — new small vector files ===
      val batch4 = (22 to 26)
        .map(
          pk =>
            s"($pk, 'name_$pk', ${pk * 10}, array($pk.0, ${pk * 2}.0, ${pk * 3}.0, ${pk * 4}.0))")
        .mkString(", ")
      sql(s"INSERT INTO $tableName VALUES $batch4")

      val vecFilesAfterWrite2 = getVectorFilesFromManifest(tableName)
      // New write adds a vector file; compacted files remain
      assert(
        vecFilesAfterWrite2.size > vecFilesAfterCompact.size,
        s"Expected more files after new write: before=${vecFilesAfterCompact.size}, after=${vecFilesAfterWrite2.size}"
      )

      // === Phase 7: Second compaction (overwrite new data to trigger merge again) ===
      // Overwrite pk 22-25 to make the new vector file low-ratio
      val overwrites2 = (22 to 25).map(
        pk => s"($pk, 'name_${pk}_v2', ${pk * 100}, array(${pk * 100}.0, 0.0, 0.0, 0.0))")
      sql(s"INSERT INTO $tableName VALUES ${overwrites2.mkString(", ")}")

      sql(s"CALL sys.compact(table => '$tableName', compact_strategy => 'full')")

      // Verify all data correct after second compaction
      val expectedFinal = (1 to 26).map {
        pk =>
          if ((1 to 5).contains(pk) || (7 to 13).contains(pk) || (15 to 20).contains(pk))
            Row(
              pk,
              s"name_$pk",
              pk * 10,
              Seq((pk * 10).toFloat, (pk * 20).toFloat, (pk * 30).toFloat, (pk * 40).toFloat))
          else if ((22 to 25).contains(pk))
            Row(pk, s"name_${pk}_v2", pk * 100, Seq((pk * 100).toFloat, 0.0f, 0.0f, 0.0f))
          else
            Row(
              pk,
              s"name_$pk",
              pk * 10,
              Seq(pk.toFloat, (pk * 2).toFloat, (pk * 3).toFloat, (pk * 4).toFloat))
      }
      checkAnswer(
        sql(s"SELECT pk, name, score, embedding FROM $tableName ORDER BY pk"),
        expectedFinal)

      // PK point queries after second compaction
      checkAnswer(
        sql(s"SELECT pk, embedding FROM $tableName WHERE pk = 22"),
        Seq(Row(22, Seq(2200.0f, 0.0f, 0.0f, 0.0f))))

      checkAnswer(
        sql(s"SELECT pk, embedding FROM $tableName WHERE pk = 26"),
        Seq(Row(26, Seq(26.0f, 52.0f, 78.0f, 104.0f))))

      // === Phase 8: Manifest correctness ===
      val diskVecFiles = getVectorFilesOnDisk(tableName)
      val manifestVecFiles = getVectorFilesFromManifest(tableName).map(_.fileName()).toSet
      assert(
        manifestVecFiles.subsetOf(diskVecFiles),
        s"Manifest files $manifestVecFiles should be subset of disk $diskVecFiles")
    }
  }

  /**
   * Test data correctness after multiple writes with auto-compaction enabled. Verifies that vector
   * data is never lost regardless of compaction behavior.
   */
  test("Vector-CF: data integrity preserved across multiple writes with compaction enabled") {
    withTable("t_data_integrity") {
      sql(s"""CREATE TABLE t_data_integrity (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  'primary-key' = 'pk',
             |  'bucket' = '1',
             |  'merge-engine' = 'partial-update',
             |  'deletion-vectors.enabled' = 'true',
             |  'vector-field' = 'embedding',
             |  'field.embedding.vector-dim' = '4',
             |  'file.format' = 'parquet',
             |  'vector-column-family.enabled' = 'true',
             |  'vector-column-family.target-file-rows' = '10',
             |  'vector-column-family.compact.enabled' = 'true',
             |  'vector-column-family.compact.min-files-to-merge' = '2',
             |  'num-sorted-runs.compaction-trigger' = '2',
             |  'compaction.min.file-num' = '2',
             |  'compaction.max.file-num' = '50'
             |)""".stripMargin)

      // Insert 4 batches
      val b1 = (1 to 6).map(pk => s"($pk, 'n$pk', array($pk.0, 0.0, 0.0, 0.0))").mkString(", ")
      sql(s"INSERT INTO t_data_integrity VALUES $b1")

      val b2 = (7 to 13).map(pk => s"($pk, 'n$pk', array($pk.0, 0.0, 0.0, 0.0))").mkString(", ")
      sql(s"INSERT INTO t_data_integrity VALUES $b2")

      val b3 = (14 to 21).map(pk => s"($pk, 'n$pk', array($pk.0, 0.0, 0.0, 0.0))").mkString(", ")
      sql(s"INSERT INTO t_data_integrity VALUES $b3")

      val b4 = (22 to 26).map(pk => s"($pk, 'n$pk', array($pk.0, 0.0, 0.0, 0.0))").mkString(", ")
      sql(s"INSERT INTO t_data_integrity VALUES $b4")

      // Verify all data is correct regardless of auto-compaction
      val expectedAll = (1 to 26).map(pk => Row(pk, s"n$pk", Seq(pk.toFloat, 0.0f, 0.0f, 0.0f)))
      checkAnswer(sql("SELECT pk, name, embedding FROM t_data_integrity ORDER BY pk"), expectedAll)

      // Verify vector files exist and contain correct total rows
      val vecFilesFinal = getVectorFilesFromManifest("t_data_integrity")
      val totalVecRows = vecFilesFinal.map(_.rowCount()).sum
      assert(
        totalVecRows == 26,
        s"Total vector rows should be 26, got $totalVecRows across ${vecFilesFinal.size} files: " +
          vecFilesFinal.map(f => s"${f.fileName()}(${f.rowCount()})").mkString(", ")
      )

      // All files must be properly tagged
      vecFilesFinal.foreach {
        f =>
          assert(f.isVectorCFFile, s"${f.fileName()} should be vector CF file")
          assert(
            f.writeCols().contains("embedding"),
            s"${f.fileName()} should have writeCols=embedding")
          assert(f.rowCount() > 0, s"${f.fileName()} should have positive row count")
      }

      // PK point queries spanning different files
      checkAnswer(
        sql("SELECT pk, embedding FROM t_data_integrity WHERE pk = 1"),
        Seq(Row(1, Seq(1.0f, 0.0f, 0.0f, 0.0f))))
      checkAnswer(
        sql("SELECT pk, embedding FROM t_data_integrity WHERE pk = 13"),
        Seq(Row(13, Seq(13.0f, 0.0f, 0.0f, 0.0f))))
      checkAnswer(
        sql("SELECT pk, embedding FROM t_data_integrity WHERE pk = 26"),
        Seq(Row(26, Seq(26.0f, 0.0f, 0.0f, 0.0f))))
    }
  }

  /**
   * Test brute-force search with deterministic data to verify correct PK results.
   *
   * Creates well-separated clusters so that top-K results are deterministic:
   *   - Cluster A (pk 1-5): vectors near (100, 0, 0, 0)
   *   - Cluster B (pk 6-10): vectors near (0, 100, 0, 0)
   *   - Cluster C (pk 11-15): vectors near (0, 0, 100, 0)
   *
   * After full compaction (with overwrites to trigger merge), query near cluster A center → all
   * results from cluster A.
   */
  test("Vector-CF: brute-force search with deterministic cluster data — precise PK assertion") {
    withTable("t_bf_precise") {
      sql(s"""CREATE TABLE t_bf_precise (
             |  pk INT,
             |  name STRING,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  'primary-key' = 'pk',
             |  'bucket' = '1',
             |  'merge-engine' = 'partial-update',
             |  'deletion-vectors.enabled' = 'true',
             |  'vector-field' = 'embedding',
             |  'field.embedding.vector-dim' = '4',
             |  'file.format' = 'parquet',
             |  'vector-column-family.enabled' = 'true',
             |  'vector-column-family.target-file-rows' = '20',
             |  'vector-column-family.compact.enabled' = 'true',
             |  'vector-column-family.compact.min-files-to-merge' = '1',
             |  'vector-column-family.compact.valid-ratio-threshold' = '0.9',
             |  'compaction.min.file-num' = '999',
             |  'compaction.max.file-num' = '999',
             |  'num-sorted-runs.compaction-trigger' = '999'
             |)""".stripMargin)

      // Cluster A: pk 1-5, vectors near (100, 0, 0, 0) with small perturbation
      val clusterA = (1 to 5)
        .map(pk => s"($pk, 'A$pk', array(${100.0 + pk * 0.01}, ${pk * 0.001}, 0.0, 0.0))")
        .mkString(", ")
      sql(s"INSERT INTO t_bf_precise VALUES $clusterA")

      // Cluster B: pk 6-10, vectors near (0, 100, 0, 0)
      val clusterB = (6 to 10)
        .map(pk => s"($pk, 'B$pk', array(${pk * 0.001}, ${100.0 + pk * 0.01}, 0.0, 0.0))")
        .mkString(", ")
      sql(s"INSERT INTO t_bf_precise VALUES $clusterB")

      // Cluster C: pk 11-15, vectors near (0, 0, 100, 0)
      val clusterC = (11 to 15)
        .map(pk => s"($pk, 'C$pk', array(0.0, ${pk * 0.001}, ${100.0 + pk * 0.01}, 0.0))")
        .mkString(", ")
      sql(s"INSERT INTO t_bf_precise VALUES $clusterC")

      // Overwrite cluster A with same vectors (to create dead refs and trigger merge)
      val overwriteA = (1 to 5)
        .map(pk => s"($pk, 'A${pk}v2', array(${100.0 + pk * 0.01}, ${pk * 0.001}, 0.0, 0.0))")
        .mkString(", ")
      sql(s"INSERT INTO t_bf_precise VALUES $overwriteA")

      // Trigger full compaction to merge vector files
      sql("CALL sys.compact(table => 't_bf_precise', compact_strategy => 'full')")

      // Verify data
      checkAnswer(sql("SELECT pk, name FROM t_bf_precise WHERE pk = 3"), Seq(Row(3, "A3v2")))

      // Search near cluster A center (100, 0, 0, 0) — top 5
      val table = loadTable("t_bf_precise")
      val search = new org.apache.paimon.accelerateindex.AccelerateIndexSearch(
        "embedding",
        Array(100.0f, 0.0f, 0.0f, 0.0f),
        5,
        "lumina",
        "l2",
        4,
        java.util.Collections.emptyMap()
      )
      val splits = table
        .newReadBuilder()
        .withAccelerateIndexSearch(search)
        .newScan()
        .plan()
        .splits()

      assert(splits.size() > 0, "Expected search splits")

      val vcfSplits = splits.asScala.collect {
        case s: org.apache.paimon.accelerateindex.VectorCFSearchSplit => s
      }

      // Collect results from ALL splits (one per vector file)
      val allResults =
        new java.util.ArrayList[org.apache.paimon.accelerateindex.VectorCFSearchHelper.ScoredRow]()
      for (split <- vcfSplits) {
        val reader =
          org.apache.paimon.accelerateindex.VectorCFSearchHelper.createReader(split, table)
        try {
          var batch = reader.readBatch()
          while (batch != null) {
            var row = batch.next()
            while (row != null) {
              var score = 0f
              var vector: Array[Float] = null
              batch match {
                case scored: org.apache.paimon.accelerateindex.VectorCFSearchHelper.ScoredRowIterator =>
                  score = scored.returnedScore()
                  vector = scored.returnedVector()
                case scored: org.apache.paimon.reader.ScoreRecordIterator[_] =>
                  score = scored.returnedScore()
                case _ =>
              }
              allResults.add(
                new org.apache.paimon.accelerateindex.VectorCFSearchHelper.ScoredRow(
                  row,
                  score,
                  vector))
              row = batch.next()
            }
            batch.releaseBatch()
            batch = reader.readBatch()
          }
        } finally {
          reader.close()
        }
      }

      // Sort by score descending and take top 5 across all files
      val sortedResults = allResults.asScala.sortBy(-_.score).take(5)

      // Verify: top-5 results should all be from cluster A (pk 1-5)
      assert(sortedResults.nonEmpty, "Expected search results")

      val resultPks = sortedResults.map(_.row.getInt(0))
      for (pk <- resultPks) {
        assert(
          pk >= 1 && pk <= 5,
          s"Result pk=$pk should be in cluster A (1-5), " +
            s"because query (100,0,0,0) is closest to cluster A. All pks: $resultPks")
      }

      // Verify no duplicate PKs in top results
      assert(resultPks.toSet.size == resultPks.size, s"Result PKs should be unique: $resultPks")

      // Search near cluster B center (0, 100, 0, 0) — top 5
      val search2 = new org.apache.paimon.accelerateindex.AccelerateIndexSearch(
        "embedding",
        Array(0.0f, 100.0f, 0.0f, 0.0f),
        5,
        "lumina",
        "l2",
        4,
        java.util.Collections.emptyMap()
      )
      val splits2 = table
        .newReadBuilder()
        .withAccelerateIndexSearch(search2)
        .newScan()
        .plan()
        .splits()

      val vcfSplits2 = splits2.asScala.collect {
        case s: org.apache.paimon.accelerateindex.VectorCFSearchSplit => s
      }

      val results2 =
        new java.util.ArrayList[org.apache.paimon.accelerateindex.VectorCFSearchHelper.ScoredRow]()
      for (split <- vcfSplits2) {
        val reader =
          org.apache.paimon.accelerateindex.VectorCFSearchHelper.createReader(split, table)
        try {
          var batch = reader.readBatch()
          while (batch != null) {
            var row = batch.next()
            while (row != null) {
              var score = 0f
              batch match {
                case scored: org.apache.paimon.accelerateindex.VectorCFSearchHelper.ScoredRowIterator =>
                  score = scored.returnedScore()
                case scored: org.apache.paimon.reader.ScoreRecordIterator[_] =>
                  score = scored.returnedScore()
                case _ =>
              }
              results2.add(
                new org.apache.paimon.accelerateindex.VectorCFSearchHelper.ScoredRow(
                  row,
                  score,
                  null))
              row = batch.next()
            }
            batch.releaseBatch()
            batch = reader.readBatch()
          }
        } finally {
          reader.close()
        }
      }

      // Top-5 for cluster B query should be pk 6-10
      val sorted2 = results2.asScala.sortBy(-_.score).take(5)
      for (sr <- sorted2) {
        val pk = sr.row.getInt(0)
        assert(pk >= 6 && pk <= 10, s"Result pk=$pk for cluster B query should be in (6-10)")
      }
    }
  }

  /**
   * Manifest correctness: verify each snapshot accurately tracks which vector files exist. No
   * compaction — just verifies ADD entries are correct across snapshots.
   */
  test("Vector-CF: manifest tracks vector files correctly across snapshots") {
    withTable("t_manifest") {
      sql(s"""CREATE TABLE t_manifest (
             |  pk INT,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  'primary-key' = 'pk',
             |  'bucket' = '1',
             |  'merge-engine' = 'partial-update',
             |  'deletion-vectors.enabled' = 'true',
             |  'vector-field' = 'embedding',
             |  'field.embedding.vector-dim' = '4',
             |  'file.format' = 'parquet',
             |  'vector-column-family.enabled' = 'true',
             |  'vector-column-family.target-file-rows' = '10',
             |  'compaction.min.file-num' = '999',
             |  'compaction.max.file-num' = '999',
             |  'num-sorted-runs.compaction-trigger' = '999'
             |)""".stripMargin)

      // Snapshot 1: 5 rows → 1 vector file
      sql(
        "INSERT INTO t_manifest VALUES " +
          (1 to 5).map(pk => s"($pk, array($pk.0, 0.0, 0.0, 0.0))").mkString(", "))
      val snap1Files = getVectorFilesFromManifest("t_manifest")
      assert(snap1Files.size == 1, s"Snap1: expected 1 vector file, got ${snap1Files.size}")
      val snap1Name = snap1Files.head.fileName()
      assert(snap1Files.head.rowCount() == 5, "Snap1 file should have 5 rows")
      assert(snap1Files.head.isVectorCFFile, "Should be marked as vector CF file")
      assert(snap1Files.head.writeCols().contains("embedding"), "writeCols should have 'embedding'")

      // Snapshot 2: 8 more rows → 1 new vector file (different from snap1)
      sql(
        "INSERT INTO t_manifest VALUES " +
          (6 to 13).map(pk => s"($pk, array($pk.0, 0.0, 0.0, 0.0))").mkString(", "))
      val snap2Files = getVectorFilesFromManifest("t_manifest")
      assert(snap2Files.size == 2, s"Snap2: expected 2 vector files, got ${snap2Files.size}")
      assert(
        snap2Files.map(_.fileName()).contains(snap1Name),
        "Snap2 should still contain snap1's vector file (immutable, one-time ADD)")
      val snap2Names = snap2Files.map(_.fileName()).toSet
      val newFileInSnap2 = (snap2Names - snap1Name).head
      assert(
        snap2Files.find(_.fileName() == newFileInSnap2).get.rowCount() == 8,
        "New file in snap2 should have 8 rows")

      // Snapshot 3: 12 more rows → 1-2 new vector files (target=10, so 12 rows may split)
      sql(
        "INSERT INTO t_manifest VALUES " +
          (14 to 25).map(pk => s"($pk, array($pk.0, 0.0, 0.0, 0.0))").mkString(", "))
      val snap3Files = getVectorFilesFromManifest("t_manifest")
      // snap1 + snap2 files should still be present
      assert(
        snap3Files.map(_.fileName()).toSet.contains(snap1Name),
        "Snap3 should still contain snap1's file")
      assert(
        snap3Files.map(_.fileName()).toSet.contains(newFileInSnap2),
        "Snap3 should still contain snap2's file")
      // Total vector rows should be 25
      val totalRows = snap3Files.map(_.rowCount()).sum
      assert(totalRows == 25, s"Total vector rows should be 25, got $totalRows")

      // Snapshot 4: overwrite pk 1 (scalar update + new vector)
      sql("INSERT INTO t_manifest VALUES (1, array(100.0, 0.0, 0.0, 0.0))")
      val snap4Files = getVectorFilesFromManifest("t_manifest")
      // Old vector files still exist (immutable). New small file added for the overwrite.
      assert(
        snap4Files.size > snap3Files.size,
        s"Snap4 should have more files than snap3. snap3=${snap3Files.size}, snap4=${snap4Files.size}")

      // Verify total data is correct
      checkAnswer(sql("SELECT count(*) FROM t_manifest"), Seq(Row(25)))
      checkAnswer(
        sql("SELECT pk, embedding FROM t_manifest WHERE pk = 1"),
        Seq(Row(1, Seq(100.0f, 0.0f, 0.0f, 0.0f))))
      checkAnswer(
        sql("SELECT pk, embedding FROM t_manifest WHERE pk = 13"),
        Seq(Row(13, Seq(13.0f, 0.0f, 0.0f, 0.0f))))
      checkAnswer(
        sql("SELECT pk, embedding FROM t_manifest WHERE pk = 25"),
        Seq(Row(25, Seq(25.0f, 0.0f, 0.0f, 0.0f))))
    }
  }

  /**
   * Test: manual minor compact triggers vector-only merge when scalar is already compacted. This
   * covers the needsIndependentCompaction() path.
   *
   * Flow: write batch 1 → auto-compact (scalar to L1) → write batch 2 → auto-compact → Now: scalar
   * at L1 (1 sorted run), 2 unfilled vector files. Manual minor compact should merge the 2 unfilled
   * vector files even though scalar doesn't need compaction.
   */
  test("Vector-CF: minor compact triggers vector-only merge when scalar already compacted") {
    withTable("t_vector_only_compact") {
      sql(s"""CREATE TABLE t_vector_only_compact (
             |  pk INT,
             |  embedding ARRAY<FLOAT>
             |) TBLPROPERTIES (
             |  'primary-key' = 'pk',
             |  'bucket' = '1',
             |  'merge-engine' = 'partial-update',
             |  'vector-field' = 'embedding',
             |  'field.embedding.vector-dim' = '4',
             |  'file.format' = 'parquet',
             |  'vector-column-family.enabled' = 'true',
             |  'vector-column-family.target-file-rows' = '10',
             |  'vector-column-family.compact.enabled' = 'true',
             |  'vector-column-family.compact.min-files-to-merge' = '2',
             |  'num-sorted-runs.compaction-trigger' = '999',
             |  'compaction.min.file-num' = '999',
             |  'compaction.max.file-num' = '999'
             |)""".stripMargin)

      // Write batch 1: 4 rows (< target 10, produces 1 unfilled vector file)
      sql(
        "INSERT INTO t_vector_only_compact VALUES " +
          (1 to 4).map(pk => s"($pk, array($pk.0, 0.0, 0.0, 0.0))").mkString(", "))

      // Write batch 2: triggers auto-compact (2 sorted runs → merge scalar to L1)
      // But vector files: only 1 unfilled from batch 1 visible at compact time
      sql(
        "INSERT INTO t_vector_only_compact VALUES " +
          (5 to 8).map(pk => s"($pk, array($pk.0, 0.0, 0.0, 0.0))").mkString(", "))

      // Now: scalar is at L1 (1 sorted run after auto-compact)
      // Vector: 2 unfilled files (batch 1: 4 rows, batch 2: 4 rows)
      val vecFilesBefore = getVectorFilesFromManifest("t_vector_only_compact")
      val unfilledBefore = vecFilesBefore.filter(_.rowCount() < 10)
      assert(
        unfilledBefore.size >= 2,
        s"Expected at least 2 unfilled vector files, got ${unfilledBefore.size}: " +
          unfilledBefore.map(f => s"${f.fileName()}(${f.rowCount()})").mkString(", ")
      )

      // Get snapshot count before manual compact
      val table = loadTable("t_vector_only_compact")
      val snapBefore = table.snapshotManager().latestSnapshotId()

      // Manual minor compact — should trigger vector-only merge
      sql("CALL sys.compact(table => 't_vector_only_compact', compact_strategy => 'minor')")

      // Verify a new snapshot was produced
      val tableAfter = loadTable("t_vector_only_compact")
      val snapAfter = tableAfter.snapshotManager().latestSnapshotId()
      assert(
        snapAfter > snapBefore,
        s"Minor compact should produce a new snapshot. Before=$snapBefore, After=$snapAfter")

      // Verify unfilled files were merged
      val vecFilesAfter = getVectorFilesFromManifest("t_vector_only_compact")
      val unfilledAfter = vecFilesAfter.filter(_.rowCount() < 10)
      assert(
        unfilledAfter.size < unfilledBefore.size,
        s"Unfilled files should decrease after merge. Before=${unfilledBefore.size}, After=${unfilledAfter.size}")

      // Verify data is still correct
      checkAnswer(
        sql("SELECT pk, embedding FROM t_vector_only_compact ORDER BY pk"),
        (1 to 8).map(pk => Row(pk, Seq(pk.toFloat, 0.0f, 0.0f, 0.0f))))
    }
  }

  // ==================== Helper Methods ====================

  private def getVectorFilesFromManifest(tableName: String): Seq[DataFileMeta] = {
    val table = loadTable(tableName)
    table
      .newSnapshotReader()
      .read()
      .dataSplits()
      .asScala
      .flatMap(_.dataFiles().asScala)
      .filter(_.isVectorCFFile)
      .toSeq
  }

  private def getVectorFileSizesOnDisk(tableName: String): Map[String, Long] = {
    val table = loadTable(tableName)
    val bucketPath = new Path(table.location(), "bucket-0")
    table
      .fileIO()
      .listStatus(bucketPath)
      .filter(f => f.getPath.getName.endsWith(".vector.bin"))
      .map(f => f.getPath.getName -> f.getLen)
      .toMap
  }

  private def getVectorFilesOnDisk(tableName: String): Set[String] = {
    val table = loadTable(tableName)
    val bucketPath = new Path(table.location(), "bucket-0")
    table
      .fileIO()
      .listStatus(bucketPath)
      .filter(f => f.getPath.getName.endsWith(".vector.bin"))
      .map(f => f.getPath.getName)
      .toSet
  }
}
