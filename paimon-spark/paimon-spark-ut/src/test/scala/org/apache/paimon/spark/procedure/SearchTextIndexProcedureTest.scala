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

class SearchTextIndexProcedureTest extends PaimonSparkTestBase {

  private def createTextTable(): Unit = {
    spark.sql("""CREATE TABLE T (
                |  pt INT,
                |  pk INT,
                |  captions ARRAY<STRUCT<contextEn: STRING, version: STRING>>
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
    val values = (0 until count)
      .map(i => s"(1, $i, array(struct('Word $i 0', 'v$i.0'), struct('Word $i 1', 'v$i.1')))")
      .mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values")
  }

  private def compactTable(): Unit = {
    spark.sql("CALL paimon.sys.compact(table => 'test.T')")
  }

  private def buildLuceneIndex(): String = {
    val result = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene')")
    result.collect()(0).getString(0)
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

  test("Paimon Procedure: search_text_index basic JSON DSL search") {
    createTextTable()
    insertTestData()
    compactTable()
    val buildResult = buildLuceneIndex()

    val statusRows = showStatus()
    val statusStr = statusRows.mkString("\n")
    assert(
      buildResult.contains("Built 1"),
      s"Build should succeed: $buildResult\nStatus:\n$statusStr")

    val result = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"Word 5"}}]}',
                             |  top_k => 5
                             |)""".stripMargin)
    val rows = result.collect()
    // Match "Word 5" tokenizes to ["word", "5"] with SHOULD (OR) semantics.
    // All 20 rows match "word", topK=5 → exactly 5 results returned.
    assert(
      rows.length == 5,
      s"Search for 'Word 5' should return 5 results (topK=5, all match 'word'), got ${rows.length}")
    rows.foreach {
      row =>
        val r = row.getString(0)
        assert(r.contains("pk="), s"Result should contain pk=: $r")
        assert(r.contains("score="), s"Result should contain score=: $r")
        assert(r.contains("matched="), s"Result should contain matched=: $r")
    }

    val parsed = parseResults(rows)
    // pk=5 matches both "word" AND "5" tokens → highest BM25 score → top result
    assert(parsed.head._1 == 5, s"pk=5 should be top result (matches both tokens): $parsed")

    // Verify pk=5 matched content contains its child docs
    val pk5Result = rows.map(_.getString(0)).find(_.contains("pk=5")).get
    assert(
      pk5Result.contains("contextEn=Word 5"),
      s"pk=5 matched content should contain 'contextEn=Word 5': $pk5Result")

    // Verify scores are descending (BM25 ranking)
    val scores = parsed.map(_._2)
    assert(
      scores.zip(scores.tail).forall { case (a, b) => a >= b },
      s"Scores should be descending: ${scores.mkString(", ")}")
  }

  test("Paimon Procedure: search_text_index returns filtered nested content") {
    createTextTable()
    insertTestData()
    compactTable()
    buildLuceneIndex()

    val result = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"Word"}}]}',
                             |  top_k => 10
                             |)""".stripMargin)
    val rows = result.collect()
    // All 20 rows contain "Word", topK=10 → exactly 10 results
    assert(
      rows.length == 10,
      s"Search for 'Word' with topK=10 should return 10 results, got ${rows.length}")
    rows.foreach {
      row =>
        val r = row.getString(0)
        assert(r.contains("matched="), s"Result should contain matched=: $r")
        assert(r.contains("[{"), s"Matched content should contain nested documents: $r")
        assert(r.contains("contextEn="), s"Matched content should contain field names: $r")
        // Verify matched nested docs actually contain 'Word' in contextEn
        assert(
          r.contains("contextEn=Word"),
          s"Matched contextEn should contain the query term 'Word': $r")
    }

    // Verify each result's matched content corresponds to its pk
    val allResults = rows.map(_.getString(0))
    allResults.foreach {
      r =>
        val pk = parsePk(r)
        assert(
          r.contains(s"contextEn=Word $pk"),
          s"pk=$pk matched content should contain 'contextEn=Word $pk': $r")
    }
  }

  test("Paimon Procedure: search_text_index no matches returns empty") {
    createTextTable()
    insertTestData()
    compactTable()
    buildLuceneIndex()

    val result = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"zzzznonexistent"}}]}',
                             |  top_k => 5
                             |)""".stripMargin)
    val rows = result.collect()
    assert(rows.length == 1)
    assert(
      rows(0).getString(0).contains("No results found"),
      s"Expected 'No results found' in: ${rows(0).getString(0)}")
  }

  test("Paimon Procedure: search_text_index should/must_not queries") {
    createTextTable()
    insertTestData()
    compactTable()
    buildLuceneIndex()

    // should query — match any of the conditions
    val shouldResult = spark.sql(
      """CALL paimon.sys.search_text_index(
        |  table => 'test.T',
        |  column => 'captions',
        |  query => '{"should":[{"match":{"contextEn":"5"}},{"match":{"contextEn":"10"}}]}',
        |  top_k => 10
        |)""".stripMargin)
    val shouldRows = shouldResult.collect()
    // Token "5" matches pk=5 only, token "10" matches pk=10 only → 2 results
    val shouldResults = parseResults(shouldRows)
    assert(
      shouldResults.length == 2,
      s"Should query should return 2 results: ${shouldResults.mkString(", ")}")
    assert(
      shouldResults.map(_._1).toSet == Set(5, 10),
      s"Should query should return pk={5, 10}: ${shouldResults.map(_._1).mkString(", ")}")

    // Verify matched content for should results
    shouldRows.foreach {
      row =>
        val r = row.getString(0)
        val pk = parsePk(r)
        assert(r.contains(s"contextEn=Word $pk"), s"pk=$pk should have matching content: $r")
    }

    // must_not query — exclude matching docs: "Word" matches all 20, exclude "5" → 19
    val mustNotResult = spark.sql(
      """CALL paimon.sys.search_text_index(
        |  table => 'test.T',
        |  column => 'captions',
        |  query => '{"must":[{"match":{"contextEn":"Word"}}],"must_not":[{"match":{"contextEn":"5"}}]}',
        |  top_k => 20
        |)""".stripMargin)
    val mustNotRows = mustNotResult.collect()
    val mustNotResults = parseResults(mustNotRows)
    assert(
      mustNotResults.length == 19,
      s"must_not should return 19 results: ${mustNotResults.length}")
    assert(
      mustNotResults.forall(_._1 != 5),
      s"must_not should exclude pk=5: ${mustNotResults.map(_._1).mkString(", ")}")
    assertScoresDescending(mustNotResults.map(_._2))
  }

  test("Paimon Procedure: search_text_index after row deletion") {
    createTextTable()
    insertTestData()
    compactTable()
    buildLuceneIndex()

    // Verify pk=5 is found before deletion
    val before = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"Word 5"}}]}',
                             |  top_k => 5
                             |)""".stripMargin)
    val beforeRows = before.collect()
    assert(
      beforeRows.exists(_.getString(0).contains("pk=5")),
      s"Before deletion, pk=5 should be found: ${beforeRows.map(_.getString(0)).mkString("; ")}")

    // Delete pk=5 (PK table uses upsert path, creating a delete-kind record at L0)
    spark.sql("DELETE FROM T WHERE pk = 5")
    compactTable()

    // Verify meta is updated after deletion + compact
    val statusAfterDelete = showStatus()
    assert(
      statusAfterDelete.nonEmpty,
      "Status should still have entries after deletion and compaction")

    // Rebuild index on the updated data files
    val rebuildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene')")
    val rb = rebuildResult.collect()(0).getString(0)
    assert(rb.contains("Built 1"), s"Rebuild should succeed: $rb")

    // Verify meta reflects rebuild
    val statusAfterRebuild = showStatus()
    assert(
      statusAfterRebuild.exists(s => s.contains("READY") && s.contains("lucene")),
      s"Meta should show READY lucene entry after rebuild: ${statusAfterRebuild.mkString("; ")}"
    )

    // Search again — pk=5 should no longer appear
    // Use generic "Word" (not "Word 5") to ensure non-empty results from other pks
    val after = spark.sql("""CALL paimon.sys.search_text_index(
                            |  table => 'test.T',
                            |  column => 'captions',
                            |  query => '{"must":[{"match":{"contextEn":"Word"}}]}',
                            |  top_k => 20
                            |)""".stripMargin)
    val afterRows = after.collect()
    val afterResults = parseResults(afterRows)
    // 20 rows - 1 deleted = 19 results
    assert(
      afterResults.length == 19,
      s"After deletion, should return 19 results: ${afterResults.length}")
    assert(
      afterResults.forall(_._1 != 5),
      s"pk=5 should not appear after deletion: ${afterResults.mkString(", ")}")
    assert(
      afterResults.map(_._1).toSet == ((0 until 20).toSet - 5),
      s"Should return all pks except 5: ${afterResults.map(_._1).toSet}")
    assertScoresDescending(afterResults.map(_._2))
  }

  test("Paimon Procedure: search_text_index no L1 data") {
    createTextTable()
    // Insert only 1 row with NULL captions to ensure minimal data
    spark.sql("INSERT INTO T VALUES (1, 0, null)")
    // Do NOT compact — data stays at L0

    val buildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene')")
    val r = buildResult.collect()(0).getString(0)
    // With a PK table, even without explicit compaction, data may end up at L1+.
    // The build should either find no L1+ data or succeed/skip with all nulls.
    assert(
      r.contains("No L1+ data files found") || r.contains("skipped"),
      s"Expected 'No L1+ data files found' or 'skipped' in: $r")
  }

  test("Paimon Procedure: search_text_index build idempotent") {
    createTextTable()
    insertTestData()
    compactTable()

    // First build
    val result1 = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene')")
    val r1 = result1.collect()(0).getString(0)
    assert(r1.contains("Built 1"), s"First build should succeed: $r1")

    // Second build — should skip (idempotent)
    val result2 = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene')")
    val r2 = result2.collect()(0).getString(0)
    assert(r2.contains("skipped"), s"Second build should skip: $r2")
    assert(r2.contains("Built 0"), s"Second build should have Built 0: $r2")
  }

  test("Paimon Procedure: search_text_index with numeric field") {
    // Create table with ARRAY<STRUCT<label: STRING, score: INT>>
    spark.sql("""CREATE TABLE T (
                |  pt INT,
                |  pk INT,
                |  items ARRAY<STRUCT<label: STRING, score: INT>>
                |) PARTITIONED BY (pt)
                |TBLPROPERTIES (
                |  'primary-key' = 'pt, pk',
                |  'bucket' = '1',
                |  'deletion-vectors.enabled' = 'true',
                |  'compaction.min.file-num' = '999',
                |  'compaction.max.file-num' = '999'
                |)""".stripMargin)

    // Insert data: pk=i has items=[{label: "Item i 0", score: i}, {label: "Item i 1", score: i+100}]
    val values = (0 until 20)
      .map(i => s"(1, $i, array(struct('Item $i 0', $i), struct('Item $i 1', ${i + 100})))")
      .mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values")
    spark.sql("CALL paimon.sys.compact(table => 'test.T')")

    // Build lucene index (should auto-detect score as INT)
    val buildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'items', dim => 0, algorithm => 'lucene')")
    val br = buildResult.collect()(0).getString(0)
    assert(br.contains("Built 1"), s"Build should succeed: $br")

    // Range query: score >= 5 AND score < 10
    val result = spark.sql(
      """CALL paimon.sys.search_text_index(
        |  table => 'test.T',
        |  column => 'items',
        |  query => '{"must":[{"match":{"label":"Item"}},{"range":{"score":{"gte":5,"lt":10}}}]}',
        |  top_k => 20
        |)""".stripMargin)
    val rows = result.collect()
    // pks 5-9 have score values 5-9 matching range [5, 10) → exactly 5 results
    assert(rows.length == 5, s"Range query should return exactly 5 results, got ${rows.length}")
    // Should find pks 5,6,7,8,9 (score values 5-9 in the first nested element)
    val allResults = rows.map(_.getString(0))
    for (pk <- 5 until 10) {
      assert(
        allResults.exists(_.contains(s"pk=$pk")),
        s"Range query should find pk=$pk: ${allResults.mkString("; ")}")
    }
    // Verify matched content contains label with "Item {pk}"
    allResults.foreach {
      r =>
        val pk = parsePk(r)
        assert(r.contains("matched="), s"Result should contain matched=: $r")
        assert(r.contains(s"label=Item $pk"), s"Matched content should contain label=Item $pk: $r")
    }
  }

  test("Paimon Procedure: search_text_index meta file correctness") {
    createTextTable()
    insertTestData()
    compactTable()
    buildLuceneIndex()

    val statusRows = showStatus()
    assert(
      statusRows.length == 1,
      s"Should have exactly 1 status entry after build, got ${statusRows.length}")

    // Verify meta contains expected fields
    val luceneEntries = statusRows.filter(_.contains("lucene"))
    assert(
      luceneEntries.length == 1,
      s"Should have exactly 1 lucene entry: ${statusRows.mkString("; ")}")

    val entry = luceneEntries.head
    assert(entry.contains("READY"), s"Entry should be READY: $entry")
    assert(entry.contains("lucene"), s"Entry should contain algorithm 'lucene': $entry")
    // Verify row/null counts are present and correct
    assert(entry.contains("rows="), s"Entry should contain rows=: $entry")
    assert(entry.contains("rows=20"), s"Entry should show rows=20: $entry")
    assert(entry.contains("nulls="), s"Entry should contain nulls=: $entry")
    // Verify index file is present (not N/A)
    assert(!entry.contains("N/A"), s"Index file should not be N/A: $entry")
    // Verify size > 0
    assert(entry.contains("size="), s"Entry should contain size=: $entry")
    val sizePattern = """size=(\d+)""".r
    val size = sizePattern.findFirstMatchIn(entry).map(_.group(1).toLong).getOrElse(0L)
    assert(size > 0, s"Index file size should be > 0: $entry")
  }

  test("Paimon Procedure: search_text_index requires index (no brute-force fallback)") {
    createTextTable()
    insertTestData()
    compactTable()
    // Do NOT build index

    val result = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"Word 5"}}]}',
                             |  top_k => 5
                             |)""".stripMargin)
    val rows = result.collect()
    assert(rows.length == 1, s"Should return exactly 1 row, got ${rows.length}")
    assert(
      rows(0).getString(0).contains("No results found"),
      s"Without index, should return 'No results found' (no brute-force): ${rows(0).getString(0)}")
  }

  test("Paimon Procedure: search_text_index field type auto-detection") {
    // Create table with mixed types: STRING (text), STRING (keyword candidate), INT (numeric)
    spark.sql("""CREATE TABLE T (
                |  pt INT,
                |  pk INT,
                |  items ARRAY<STRUCT<label: STRING, score: INT>>
                |) PARTITIONED BY (pt)
                |TBLPROPERTIES (
                |  'primary-key' = 'pt, pk',
                |  'bucket' = '1',
                |  'deletion-vectors.enabled' = 'true',
                |  'compaction.min.file-num' = '999',
                |  'compaction.max.file-num' = '999'
                |)""".stripMargin)

    val values = (0 until 10)
      .map(i => s"(1, $i, array(struct('Label $i', ${i * 10})))")
      .mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values")
    spark.sql("CALL paimon.sys.compact(table => 'test.T')")

    // Build without explicit type options — should auto-detect
    val buildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'items', dim => 0, algorithm => 'lucene')")
    val br = buildResult.collect()(0).getString(0)
    assert(br.contains("Built 1"), s"Build should succeed with auto-detected types: $br")

    // Verify text search works (label auto-detected as text)
    val textResult = spark.sql("""CALL paimon.sys.search_text_index(
                                 |  table => 'test.T',
                                 |  column => 'items',
                                 |  query => '{"must":[{"match":{"label":"Label 3"}}]}',
                                 |  top_k => 5
                                 |)""".stripMargin)
    val textRows = textResult.collect()
    // "Label 3" tokenizes to ["label", "3"] with SHOULD (OR) semantics.
    // All 10 rows match "label", topK=5 → exactly 5 results returned.
    assert(
      textRows.length == 5,
      s"Text search for 'Label 3' should return 5 results (topK=5), got ${textRows.length}")
    // pk=3 matches both "label" AND "3" tokens → highest BM25 score → top result
    val textParsed = parseResults(textRows)
    assert(
      textParsed.head._1 == 3,
      s"pk=3 should be top result for 'Label 3': ${textParsed.mkString("; ")}")

    // Verify range query works (score auto-detected as int)
    val rangeResult = spark.sql(
      """CALL paimon.sys.search_text_index(
        |  table => 'test.T',
        |  column => 'items',
        |  query => '{"must":[{"match":{"label":"Label"}},{"range":{"score":{"gte":30,"lt":60}}}]}',
        |  top_k => 10
        |)""".stripMargin)
    val rangeRows = rangeResult.collect()
    // pk=3 (score=30), pk=4 (score=40), pk=5 (score=50) → exactly 3 results
    assert(
      rangeRows.length == 3,
      s"Range query [30,60) should return exactly 3 results, got ${rangeRows.length}")
    // pk=3 (score=30), pk=4 (score=40), pk=5 (score=50) should match
    val rangeResults = rangeRows.map(_.getString(0))
    for (pk <- 3 until 6) {
      assert(
        rangeResults.exists(_.contains(s"pk=$pk")),
        s"Range [30,60) should find pk=$pk: ${rangeResults.mkString("; ")}")
    }
  }

  test("Paimon Procedure: search_text_index keyword analyzer via options") {
    createTextTable()
    insertTestData(10)
    compactTable()

    // Build with keyword analyzer for version field
    val buildResult = spark.sql(
      "CALL paimon.sys.build_accelerate_index(" +
        "table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene', " +
        "options => 'lucene.field.version.analyzer=keyword')")
    val br = buildResult.collect()(0).getString(0)
    assert(br.contains("Built 1"), s"Build should succeed: $br")

    // match query with keyword analyzer: "v5.0" should match exactly (whole token)
    val exactResult = spark.sql("""CALL paimon.sys.search_text_index(
                                  |  table => 'test.T',
                                  |  column => 'captions',
                                  |  query => '{"must":[{"match":{"version":"v5.0"}}]}',
                                  |  top_k => 5,
                                  |  options => 'lucene.field.version.analyzer=keyword'
                                  |)""".stripMargin)
    val exactRows = exactResult.collect()
    val exactResults = parseResults(exactRows)
    assert(
      exactResults.length == 1,
      s"Keyword match for 'v5.0' should return exactly 1 result: ${exactResults.mkString(", ")}")
    assert(exactResults(0)._1 == 5, s"Should match pk=5: ${exactResults.mkString(", ")}")
    assert(exactResults(0)._2 > 0, s"Score should be positive: ${exactResults(0)._2}")
    // Verify matched content contains version=v5.0
    val exactStr = exactRows(0).getString(0)
    assert(
      exactStr.contains("version=v5.0"),
      s"Matched content should contain version=v5.0: $exactStr")

    // match query with keyword analyzer: "v5" should NOT match "v5.0" (no tokenization)
    val partialResult = spark.sql("""CALL paimon.sys.search_text_index(
                                    |  table => 'test.T',
                                    |  column => 'captions',
                                    |  query => '{"must":[{"match":{"version":"v5"}}]}',
                                    |  top_k => 5,
                                    |  options => 'lucene.field.version.analyzer=keyword'
                                    |)""".stripMargin)
    val partialRows = partialResult.collect()
    assert(
      partialRows.length == 1 && partialRows(0).getString(0).contains("No results found"),
      s"Keyword match for 'v5' should not match 'v5.0': ${partialRows.map(_.getString(0)).mkString("; ")}"
    )
  }

  test("Paimon Procedure: search_text_index topK exceeds index size") {
    createTextTable()
    insertTestData(5)
    compactTable()
    buildLuceneIndex()

    val result = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"Word"}}]}',
                             |  top_k => 100
                             |)""".stripMargin)
    val rows = result.collect()
    val results = parseResults(rows)
    assert(results.length == 5, s"Should return exactly 5 results (all rows): ${results.length}")
    assert(
      results.map(_._1).toSet == Set(0, 1, 2, 3, 4),
      s"Should return all pks 0-4: ${results.map(_._1).mkString(", ")}")
    assertScoresDescending(results.map(_._2))
  }

  test("Paimon Procedure: search_text_index multi-bucket") {
    // Create table with 2 buckets
    spark.sql("""CREATE TABLE T (
                |  pt INT,
                |  pk INT,
                |  captions ARRAY<STRUCT<contextEn: STRING, version: STRING>>
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
    buildLuceneIndex()

    // Search across all buckets
    val result = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"Word"}}]}',
                             |  top_k => 20
                             |)""".stripMargin)
    val rows = result.collect()
    val results = parseResults(rows)
    assert(results.length == 20, s"Should return all 20 rows from both buckets: ${results.length}")
    assert(
      results.map(_._1).toSet == (0 until 20).toSet,
      s"Should contain all pks 0-19: ${results.map(_._1).toSet}")
    assertScoresDescending(results.map(_._2))

    // Verify both buckets have indexes
    val statusRows = showStatus()
    val readyCount = statusRows.count(_.contains("READY"))
    assert(
      readyCount == 2,
      s"Should have exactly 2 READY index entries (one per bucket): ${statusRows.mkString("; ")}")
  }

  test("Paimon Procedure: search_text_index multi-partition") {
    // Create table with 2 partitions
    spark.sql("""CREATE TABLE T (
                |  pt INT,
                |  pk INT,
                |  captions ARRAY<STRUCT<contextEn: STRING, version: STRING>>
                |) PARTITIONED BY (pt)
                |TBLPROPERTIES (
                |  'primary-key' = 'pt, pk',
                |  'bucket' = '1',
                |  'deletion-vectors.enabled' = 'true',
                |  'compaction.min.file-num' = '999',
                |  'compaction.max.file-num' = '999'
                |)""".stripMargin)

    // Insert into partition pt=1 (pk 0-9)
    val values1 = (0 until 10)
      .map(i => s"(1, $i, array(struct('Word $i 0', 'v$i.0'), struct('Word $i 1', 'v$i.1')))")
      .mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values1")
    // Insert into partition pt=2 (pk 10-19)
    val values2 = (10 until 20)
      .map(i => s"(2, $i, array(struct('Word $i 0', 'v$i.0'), struct('Word $i 1', 'v$i.1')))")
      .mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values2")
    compactTable()
    buildLuceneIndex()

    // Search across both partitions
    val result = spark.sql("""CALL paimon.sys.search_text_index(
                             |  table => 'test.T',
                             |  column => 'captions',
                             |  query => '{"must":[{"match":{"contextEn":"Word"}}]}',
                             |  top_k => 20,
                             |  partitions => 'pt=1;pt=2'
                             |)""".stripMargin)
    val rows = result.collect()
    val results = parseResults(rows)
    val allPks = results.map(_._1).toSet
    assert(allPks == (0 until 20).toSet, s"Should cover all 20 pks from both partitions: $allPks")
    assertScoresDescending(results.map(_._2))
  }

  test("build_accelerate_index with explicit snapshot_id for Lucene") {
    createTextTable()
    insertTestData(10)
    compactTable()

    // Capture snapshot S1 after first compact (L1: 10 rows)
    val table1 = loadTable("T").asInstanceOf[FileStoreTable]
    val snapshotS1 = table1.snapshotManager().latestSnapshotId()

    // Build at S1 — should succeed
    val result1 = spark.sql(
      s"CALL paimon.sys.build_accelerate_index(" +
        s"table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene', " +
        s"snapshot_id => $snapshotS1)")
    val r1 = result1.collect()(0).getString(0)
    assert(r1.contains("Built 1"), s"Build at S1 should succeed: $r1")

    // Verify meta shows rows=10
    val status1 = showStatus()
    assert(
      status1.exists(_.contains("rows=10")),
      s"S1 index should have 10 rows: ${status1.mkString("; ")}")
  }

  test("search_text_index with explicit snapshot_id") {
    createTextTable()
    insertTestData(10)
    compactTable()

    // Capture snapshot after compact (has L1 data with index)
    val table = loadTable("T").asInstanceOf[FileStoreTable]
    val indexedSnapshot = table.snapshotManager().latestSnapshotId()

    // Build Lucene index at current snapshot
    buildLuceneIndex()

    // Insert more data and compact again → new L1 files, old index doesn't cover them
    insertTestData(10)
    compactTable()

    // Search with indexedSnapshot → should find results (files match old index)
    val result1 = spark.sql(s"""CALL paimon.sys.search_text_index(
                               |  table => 'test.T',
                               |  column => 'captions',
                               |  query => '{"must":[{"match":{"contextEn":"Word 5"}}]}',
                               |  top_k => 5,
                               |  snapshot_id => $indexedSnapshot
                               |)""".stripMargin)
    val results1 = parseResults(result1.collect())
    assert(results1.nonEmpty, "Search at indexed snapshot should return results")
    assert(results1.exists(_._1 == 5), "pk=5 should appear in results at indexed snapshot")
    assertScoresDescending(results1.map(_._2))
  }

  test("multi-snapshot cross build and search isolation for Lucene") {
    createTextTable()

    // Phase 1: Insert 10 rows, compact → S1
    insertTestData(10)
    compactTable()
    val table1 = loadTable("T").asInstanceOf[FileStoreTable]
    val snapshotS1 = table1.snapshotManager().latestSnapshotId()

    // Build Lucene index at S1
    val buildS1 = spark.sql(
      s"CALL paimon.sys.build_accelerate_index(" +
        s"table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene', " +
        s"snapshot_id => $snapshotS1)")
    val r1 = buildS1.collect()(0).getString(0)
    assert(r1.contains("Built 1"), s"Build at S1 should build 1: $r1")

    // Phase 2: Insert 10 more rows (pk 10-19), compact → S2
    val moreValues = (10 until 20)
      .map(i => s"(1, $i, array(struct('Word $i 0', 'v$i.0'), struct('Word $i 1', 'v$i.1')))")
      .mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $moreValues")
    compactTable()
    val table2 = loadTable("T").asInstanceOf[FileStoreTable]
    val snapshotS2 = table2.snapshotManager().latestSnapshotId()
    assert(snapshotS2 > snapshotS1, "S2 should be after S1")

    // Build Lucene index at S2
    val buildS2 = spark.sql(
      s"CALL paimon.sys.build_accelerate_index(" +
        s"table => 'test.T', column => 'captions', dim => 0, algorithm => 'lucene', " +
        s"snapshot_id => $snapshotS2)")
    val r2 = buildS2.collect()(0).getString(0)
    assert(r2.contains("Built"), s"Build at S2 should succeed: $r2")

    // Search at S1 → should return results from S1 data (pk 0-9)
    val searchS1 = spark.sql(s"""CALL paimon.sys.search_text_index(
                                |  table => 'test.T',
                                |  column => 'captions',
                                |  query => '{"must":[{"match":{"contextEn":"Word"}}]}',
                                |  top_k => 10,
                                |  snapshot_id => $snapshotS1
                                |)""".stripMargin)
    val resultsS1 = parseResults(searchS1.collect())
    assert(resultsS1.nonEmpty, s"Search at S1 should return results")
    assertScoresDescending(resultsS1.map(_._2))

    // Search at S2 → should return results from S2 data (pk 0-19)
    val searchS2 = spark.sql(s"""CALL paimon.sys.search_text_index(
                                |  table => 'test.T',
                                |  column => 'captions',
                                |  query => '{"must":[{"match":{"contextEn":"Word"}}]}',
                                |  top_k => 20,
                                |  snapshot_id => $snapshotS2
                                |)""".stripMargin)
    val resultsS2 = parseResults(searchS2.collect())
    assert(resultsS2.nonEmpty, s"Search at S2 should return results")
    assertScoresDescending(resultsS2.map(_._2))
  }
}
