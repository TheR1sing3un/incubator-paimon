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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.Row

class AccelerateIndexProcedureTest extends PaimonSparkTestBase {

  private lazy val isLuminaAvailable: Boolean = {
    try {
      // Triggers static initializer which loads the native library via JNI
      Class.forName("org.aliyun.lumina.LuminaNative")
      true
    } catch {
      case _: Throwable => false
    }
  }

  private def createVectorTable(): Unit = {
    spark.sql("""CREATE TABLE T (
                |  pt INT,
                |  pk INT,
                |  vec ARRAY<FLOAT>
                |) PARTITIONED BY (pt)
                |TBLPROPERTIES (
                |  'primary-key' = 'pt, pk',
                |  'bucket' = '1',
                |  'deletion-vectors.enabled' = 'true',
                |  'compaction.min.file-num' = '999',
                |  'compaction.max.file-num' = '999'
                |)""".stripMargin)
  }

  private def insertTestData(count: Int = 20): Unit = {
    val values =
      (0 until count).map(i => s"(1, $i, array(${i.toFloat}F, 0.0F, 0.0F, 0.0F))").mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values")
  }

  private def compactTable(): Unit = {
    spark.sql("CALL paimon.sys.compact(table => 'test.T')")
  }

  private def showStatus(): Array[String] = {
    val status = spark.sql("CALL paimon.sys.show_accelerate_index_status(table => 'test.T')")
    status.collect().map(_.getString(0))
  }

  private def parseScore(result: String): Float = {
    val scorePattern = """score=([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)""".r
    scorePattern.findFirstMatchIn(result).map(_.group(1).toFloat).getOrElse(0f)
  }

  private def parsePk(result: String): Int = {
    val pkPattern = """pk=(\d+)""".r
    pkPattern.findFirstMatchIn(result).map(_.group(1).toInt).getOrElse(-1)
  }

  private def parseResults(rows: Array[Row]): Seq[(Int, Float)] = {
    rows
      .map(_.getString(0))
      .filter(!_.contains("No results found"))
      .map(r => (parsePk(r), parseScore(r)))
  }

  private def assertScoresDescending(scores: Seq[Float]): Unit = {
    for (i <- 0 until scores.length - 1) {
      assert(scores(i) >= scores(i + 1), s"Scores should be descending: ${scores.mkString(", ")}")
    }
  }

  test("Paimon Procedure: build_accelerate_index basic") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData()
    compactTable()

    val result = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")
    val rows = result.collect()
    assert(rows.length == 1)
    val resultStr = rows(0).getString(0)
    assert(resultStr.contains("Built 1"), s"Expected 'Built 1' in: $resultStr")
  }

  test("Paimon Procedure: show_accelerate_index_status after build") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData()
    compactTable()

    spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")

    val statusRows = showStatus()
    assert(statusRows.length == 1, s"Expected exactly 1 status row, got ${statusRows.length}")
    assert(
      statusRows.exists(_.contains("READY")),
      s"Expected 'READY' in status: ${statusRows.mkString(", ")}")
    assert(
      statusRows.exists(_.contains("lumina")),
      s"Expected 'lumina' in status: ${statusRows.mkString(", ")}")
  }

  test("Paimon Procedure: show_accelerate_index_status with state filter") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData()
    compactTable()

    spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")

    // Filter by READY
    val readyStatus = spark.sql(
      "CALL paimon.sys.show_accelerate_index_status(" +
        "table => 'test.T', state => 'READY')")
    val readyRows = readyStatus.collect()
    assert(readyRows.length == 1, s"Expected 1 READY entry, got ${readyRows.length}")
    readyRows.foreach(row => assert(row.getString(0).contains("READY")))

    // Filter by FAILED — should find nothing
    val failedStatus = spark.sql(
      "CALL paimon.sys.show_accelerate_index_status(" +
        "table => 'test.T', state => 'FAILED')")
    val failedRows = failedStatus.collect()
    assert(failedRows.length == 1)
    assert(failedRows(0).getString(0).contains("No accelerate index entries found"))
  }

  test("Paimon Procedure: build_accelerate_index idempotent") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData()
    compactTable()

    // First build
    val result1 = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")
    val r1 = result1.collect()(0).getString(0)
    assert(r1.contains("Built 1"), s"First build should build 1 index: $r1")

    // Second build — should skip (idempotent)
    val result2 = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")
    val r2 = result2.collect()(0).getString(0)
    assert(r2.contains("skipped"), s"Second build should skip: $r2")
    assert(r2.contains("Built 0"), s"Second build should have Built 0: $r2")
  }

  test("Paimon Procedure: build_accelerate_index no L1 data") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    // Insert only 1 row with NULL vector to ensure minimal data
    spark.sql("INSERT INTO T VALUES (1, 0, null)")
    // Do NOT compact — data stays at L0

    val result = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")
    val r = result.collect()(0).getString(0)
    // With a PK table, even without explicit compaction, data may end up at L1+.
    // The build should either find no L1+ data or succeed/skip with all nulls.
    assert(
      r.contains("No L1+ data files found") || r.contains("skipped"),
      s"Expected 'No L1+ data files found' or 'skipped' in: $r")
  }

  test("Paimon Procedure: search_accelerate_index basic with content verification") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData()
    compactTable()

    spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")

    val result = spark.sql(
      "CALL paimon.sys.search_accelerate_index(" +
        "table => 'test.T', column => 'vec', " +
        "query_vector => '1.0,0.0,0.0,0.0', top_k => 5, dim => 4)")
    val rows = result.collect()
    assert(rows.length == 5, s"Search should return exactly 5 results, got ${rows.length}")
    rows.foreach {
      row =>
        val r = row.getString(0)
        assert(r.contains("pk="), s"Result should contain pk=: $r")
        assert(r.contains("score="), s"Result should contain score=: $r")
        assert(r.contains("vector="), s"Result should contain vector=: $r")
    }

    val allResults = rows.map(_.getString(0))
    // Verify data correctness: query [1,0,0,0] should find pk=1 (vec=[1,0,0,0]) as top result
    assert(
      allResults(0).contains("pk=1"),
      s"Top result should be pk=1 (exact match for [1,0,0,0]), got: ${allResults(0)}")

    // Verify top result vector content matches pk=1's data: [1.0, 0.0, 0.0, 0.0]
    assert(
      allResults(0).contains("vector=[1.0"),
      s"Top result vector should start with [1.0: ${allResults(0)}")

    // Verify scores are positive and ordered descending
    val scores = allResults.map(parseScore)
    assert(scores.forall(_ > 0), s"All scores should be positive: ${scores.mkString(", ")}")
    for (i <- 0 until scores.length - 1) {
      assert(
        scores(i) >= scores(i + 1),
        s"Results should be ordered by descending score: ${scores.mkString(", ")}")
    }

    // Verify top score is significantly higher than last (pk=1 is exact match)
    if (scores.length > 1) {
      assert(
        scores(0) > scores(scores.length - 1),
        s"Top score should be higher than last: ${scores.mkString(", ")}")
    }
  }

  test("Paimon Procedure: build_accelerate_index meta file correctness") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData()
    compactTable()

    val buildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")
    val br = buildResult.collect()(0).getString(0)
    assert(br.contains("Built 1"), s"Build should build 1 index: $br")

    val statusRows = showStatus()
    assert(statusRows.length == 1, s"Expected 1 status entry, got ${statusRows.length}")

    val luminaEntries = statusRows.filter(_.contains("lumina"))
    assert(
      luminaEntries.length == 1,
      s"Expected 1 lumina entry, got ${luminaEntries.length}: ${statusRows.mkString("; ")}")

    val entry = luminaEntries.head
    // Verify essential meta fields
    assert(entry.contains("READY"), s"Entry should be READY: $entry")
    assert(entry.contains("lumina"), s"Entry should contain algorithm 'lumina': $entry")
    assert(entry.contains("col="), s"Entry should contain col=: $entry")
    assert(entry.contains("rows="), s"Entry should contain rows=: $entry")
    assert(entry.contains("nulls="), s"Entry should contain nulls=: $entry")
    assert(entry.contains("files="), s"Entry should contain files=: $entry")
    // Verify index file exists (not N/A)
    assert(!entry.contains("N/A"), s"Index file should not be N/A: $entry")
    // Verify size > 0
    val sizePattern = """size=(\d+)""".r
    val size = sizePattern.findFirstMatchIn(entry).map(_.group(1).toLong).getOrElse(0L)
    assert(size > 0, s"Index file size should be > 0: $entry")
    // Verify rows count matches inserted data
    val rowsPattern = """rows=(\d+)""".r
    val rows = rowsPattern.findFirstMatchIn(entry).map(_.group(1).toLong).getOrElse(0L)
    assert(rows == 20, s"Should have 20 rows: $entry")
  }

  test("Paimon Procedure: search_accelerate_index after deletion with DV") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData()
    compactTable()

    spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")

    // Verify pk=1 is found before deletion (query=[1,0,0,0] matches vec=[1,0,0,0])
    val before = spark.sql(
      "CALL paimon.sys.search_accelerate_index(" +
        "table => 'test.T', column => 'vec', " +
        "query_vector => '1.0,0.0,0.0,0.0', top_k => 3, dim => 4)")
    val beforeRows = before.collect()
    assert(
      beforeRows.exists(_.getString(0).contains("pk=1")),
      s"Before deletion, pk=1 should be found: ${beforeRows.map(_.getString(0)).mkString("; ")}")

    // Delete pk=1
    spark.sql("DELETE FROM T WHERE pk = 1")
    compactTable()

    // Rebuild index
    val rebuildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")
    val rb = rebuildResult.collect()(0).getString(0)
    assert(rb.contains("Built 1"), s"Rebuild should build 1 index: $rb")

    // Verify meta is updated after rebuild
    val statusAfterRebuild = showStatus()
    assert(
      statusAfterRebuild.exists(s => s.contains("READY") && s.contains("lumina")),
      s"Meta should show READY lumina entry: ${statusAfterRebuild.mkString("; ")}"
    )

    // Search again — pk=1 should no longer be top result
    val after = spark.sql(
      "CALL paimon.sys.search_accelerate_index(" +
        "table => 'test.T', column => 'vec', " +
        "query_vector => '1.0,0.0,0.0,0.0', top_k => 3, dim => 4)")
    val afterRows = after.collect()
    val afterResults = parseResults(afterRows)
    assert(
      afterResults.length == 3,
      s"After deletion, search should return exactly 3 results (topK=3), got ${afterResults.length}")
    assert(
      afterResults.forall(_._1 != 1),
      s"pk=1 should not appear after deletion: ${afterResults.mkString(", ")}")
    assertScoresDescending(afterResults.map(_._2))
  }

  test("Paimon Procedure: build_accelerate_index null vectors skipped with meta") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    // Insert all null vectors
    val values = (0 until 10).map(i => s"(1, $i, null)").mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values")
    compactTable()

    val buildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")
    val r = buildResult.collect()(0).getString(0)
    assert(r.contains("skipped"), s"All-null build should be skipped: $r")

    // Verify meta reflects skip: SKIPPED state with nulls=10
    val statusRows = showStatus()
    val hasSkipped = statusRows.exists(s => s.contains("SKIPPED") && s.contains("nulls=10"))
    assert(hasSkipped, s"Meta should show SKIPPED with nulls=10: ${statusRows.mkString("; ")}")
  }

  test("Paimon Procedure: search_accelerate_index topK exceeds index size") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData(5)
    compactTable()

    spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")

    // Search with topK=100, but only 5 rows exist
    val result = spark.sql(
      "CALL paimon.sys.search_accelerate_index(" +
        "table => 'test.T', column => 'vec', " +
        "query_vector => '1.0,0.0,0.0,0.0', top_k => 100, dim => 4)")
    val rows = result.collect()
    val results = parseResults(rows)
    assert(results.length == 5, s"Should return exactly 5 results (all rows): ${results.length}")
    assert(
      results(0)._1 == 1,
      s"Top result should be pk=1 (exact match for [1,0,0,0]): ${results(0)}")
    assert(
      results(0)._2 > results.last._2,
      s"Top score should be higher than last: ${results.map(_._2).mkString(", ")}")
    assertScoresDescending(results.map(_._2))
  }

  test("Paimon Procedure: search_accelerate_index multi-bucket") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    // Create table with 2 buckets
    spark.sql("""CREATE TABLE T (
                |  pt INT,
                |  pk INT,
                |  vec ARRAY<FLOAT>
                |) PARTITIONED BY (pt)
                |TBLPROPERTIES (
                |  'primary-key' = 'pt, pk',
                |  'bucket' = '2',
                |  'deletion-vectors.enabled' = 'true',
                |  'compaction.min.file-num' = '999',
                |  'compaction.max.file-num' = '999'
                |)""".stripMargin)
    insertTestData(20)
    compactTable()

    spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")

    // Search across all buckets
    val result = spark.sql(
      "CALL paimon.sys.search_accelerate_index(" +
        "table => 'test.T', column => 'vec', " +
        "query_vector => '1.0,0.0,0.0,0.0', top_k => 5, dim => 4)")
    val rows = result.collect()
    val results = parseResults(rows)
    assert(results.length == 5, s"Should return 5 results: ${results.length}")
    assert(results(0)._1 == 1, s"Top result should be pk=1 (exact match): ${results(0)}")
    assertScoresDescending(results.map(_._2))

    // Verify both buckets have indexes
    val statusRows = showStatus()
    val readyCount = statusRows.count(_.contains("READY"))
    assert(
      readyCount == 2,
      s"Should have exactly 2 READY index entries: ${statusRows.mkString("; ")}")
  }

  test("build_accelerate_index with explicit snapshot_id") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData(10)
    compactTable()

    // Capture snapshot S1 after first compact (L1: 10 rows)
    val table1 = loadTable("T").asInstanceOf[FileStoreTable]
    val snapshotS1 = table1.snapshotManager().latestSnapshotId()

    // Build at S1 — should succeed
    val result1 = spark.sql(
      s"CALL paimon.sys.build_accelerate_index(" +
        s"table => 'test.T', column => 'vec', dim => 4, " +
        s"snapshot_id => $snapshotS1)")
    val r1 = result1.collect()(0).getString(0)
    assert(r1.contains("Built 1"), s"Build at S1 should succeed: $r1")

    // Verify meta shows rows=10
    val status1 = showStatus()
    assert(
      status1.exists(_.contains("rows=10")),
      s"S1 index should have 10 rows: ${status1.mkString("; ")}")
  }

  test("search_accelerate_index with explicit snapshot_id") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()
    insertTestData(10)
    compactTable()

    // Capture snapshot after compact (has L1 data with index)
    val table = loadTable("T").asInstanceOf[FileStoreTable]
    val indexedSnapshot = table.snapshotManager().latestSnapshotId()

    // Build index
    spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'vec', dim => 4)")

    // Insert more data and compact again → new L1 files, old index doesn't cover them
    insertTestData(10)
    compactTable()

    // Search with indexedSnapshot → should find results (files match old index)
    val result1 = spark.sql(
      s"CALL paimon.sys.search_accelerate_index(" +
        s"table => 'test.T', column => 'vec', " +
        s"query_vector => '1.0,0.0,0.0,0.0', top_k => 5, dim => 4, " +
        s"snapshot_id => $indexedSnapshot)")
    val results1 = parseResults(result1.collect())
    assert(results1.nonEmpty, "Search at indexed snapshot should return results")
    assertScoresDescending(results1.map(_._2))
  }

  test("multi-snapshot cross build and search isolation") {
    assume(isLuminaAvailable, "Lumina native library not available on this platform")
    createVectorTable()

    // Phase 1: Insert 10 rows, compact → S1
    insertTestData(10)
    compactTable()
    val table1 = loadTable("T").asInstanceOf[FileStoreTable]
    val snapshotS1 = table1.snapshotManager().latestSnapshotId()

    // Build index at S1
    val buildS1 = spark.sql(
      s"CALL paimon.sys.build_accelerate_index(" +
        s"table => 'test.T', column => 'vec', dim => 4, " +
        s"snapshot_id => $snapshotS1)")
    val r1 = buildS1.collect()(0).getString(0)
    assert(r1.contains("Built 1"), s"Build at S1 should build 1: $r1")

    // Phase 2: Insert 10 more rows (pk 0-9 again, will overwrite), compact → S2
    val moreValues =
      (0 until 10)
        .map(i => s"(1, ${i + 10}, array(${(i + 10).toFloat}F, 0.0F, 0.0F, 0.0F))")
        .mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $moreValues")
    compactTable()
    val table2 = loadTable("T").asInstanceOf[FileStoreTable]
    val snapshotS2 = table2.snapshotManager().latestSnapshotId()
    assert(snapshotS2 > snapshotS1, "S2 should be after S1")

    // Build index at S2
    val buildS2 = spark.sql(
      s"CALL paimon.sys.build_accelerate_index(" +
        s"table => 'test.T', column => 'vec', dim => 4, " +
        s"snapshot_id => $snapshotS2)")
    val r2 = buildS2.collect()(0).getString(0)
    assert(r2.contains("Built"), s"Build at S2 should succeed: $r2")

    // Search at S1 → should return results from S1 data (pk 0-9)
    val searchS1 = spark.sql(
      s"CALL paimon.sys.search_accelerate_index(" +
        s"table => 'test.T', column => 'vec', " +
        s"query_vector => '1.0,0.0,0.0,0.0', top_k => 5, dim => 4, " +
        s"snapshot_id => $snapshotS1)")
    val resultsS1 = parseResults(searchS1.collect())
    assert(resultsS1.nonEmpty, s"Search at S1 should return results")
    assertScoresDescending(resultsS1.map(_._2))

    // Search at S2 → should return results from S2 data (pk 0-19)
    val searchS2 = spark.sql(
      s"CALL paimon.sys.search_accelerate_index(" +
        s"table => 'test.T', column => 'vec', " +
        s"query_vector => '1.0,0.0,0.0,0.0', top_k => 5, dim => 4, " +
        s"snapshot_id => $snapshotS2)")
    val resultsS2 = parseResults(searchS2.collect())
    assert(resultsS2.nonEmpty, s"Search at S2 should return results")
    assertScoresDescending(resultsS2.map(_._2))
  }
}
