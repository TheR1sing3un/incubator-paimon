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

import org.apache.spark.sql.Row

import java.io.{File, PrintWriter}
import java.sql.{Date, Timestamp}

class LoadFileProcedureTest extends PaimonSparkTestBase {

  private def writeText(dir: File, name: String, lines: String*): Unit = {
    val f = new File(dir, name)
    val w = new PrintWriter(f)
    try lines.foreach(w.println)
    finally w.close()
  }

  test("Paimon procedure: load_file csv into partitioned table") {
    withTempDir {
      dir =>
        writeText(dir, "a.csv", "id,name,dt", "1,a,2026-04-01", "2,b,2026-04-02")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING, dt STRING)
                     |USING PAIMON PARTITIONED BY (dt)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'csv', options => map('header', 'true', 'sep', ','))"),
          Row(true, 2L, 0L) :: Nil
        )

        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY id"),
          Row(1, "a", "2026-04-01") :: Row(2, "b", "2026-04-02") :: Nil)
    }
  }

  test("Paimon procedure: load_file jsonl into non-partitioned table") {
    withTempDir {
      dir =>
        writeText(dir, "a.jsonl", """{"id":1,"name":"a"}""", """{"id":2,"name":"b"}""")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING)
                     |USING PAIMON
                     |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'jsonl')"),
          Row(true, 2L, 0L) :: Nil)

        checkAnswer(spark.sql("SELECT * FROM T ORDER BY id"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("Paimon procedure: load_file jsonl fills missing column with null") {
    withTempDir {
      dir =>
        writeText(dir, "a.jsonl", """{"id":1}""", """{"id":2,"name":"b"}""")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING)
                     |USING PAIMON
                     |""".stripMargin)

        spark.sql(
          s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}', format => 'jsonl')")

        checkAnswer(spark.sql("SELECT * FROM T ORDER BY id"), Row(1, null) :: Row(2, "b") :: Nil)
    }
  }

  test("Paimon procedure: load_file reads a directory of multiple files") {
    withTempDir {
      dir =>
        writeText(dir, "part-1.csv", "id,name", "1,a", "2,b")
        writeText(dir, "part-2.csv", "id,name", "3,c", "4,d")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING)
                     |USING PAIMON
                     |""".stripMargin)

        spark.sql(
          s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
            s" format => 'csv', options => map('header', 'true', 'sep', ','))")

        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY id"),
          Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
    }
  }

  test("Paimon procedure: load_file csv rich scalar types with quotes / commas / nulls") {
    withTempDir {
      dir =>
        // header + rows exercising: quoted string with embedded comma, empty -> null,
        // decimal, double, date, timestamp.
        writeText(
          dir,
          "rich.csv",
          "id,name,amount,ratio,dt,ts",
          """1,"Smith, John",123.45,0.125,2026-04-01,2026-04-01 10:20:30""",
          """2,"O""Neil",,2.5,2026-04-02,2026-04-02 00:00:00""",
          """3,plain,7.00,,2026-04-03,2026-04-03 23:59:59"""
        )

        spark.sql(s"""
                     |CREATE TABLE T (
                     |  id INT,
                     |  name STRING,
                     |  amount DECIMAL(10,2),
                     |  ratio DOUBLE,
                     |  dt DATE,
                     |  ts TIMESTAMP
                     |) USING PAIMON PARTITIONED BY (dt)
                     |""".stripMargin)

        val dq = "\""
        spark.sql(
          s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
            s" format => 'csv', options => map('header','true','sep',',','escape','$dq'))")

        checkAnswer(
          spark.sql("SELECT id, name, amount, ratio, dt, ts FROM T ORDER BY id"),
          Row(
            1,
            "Smith, John",
            new java.math.BigDecimal("123.45"),
            0.125d,
            Date.valueOf("2026-04-01"),
            Timestamp.valueOf("2026-04-01 10:20:30")) ::
            Row(
              2,
              "O\"Neil",
              null,
              2.5d,
              Date.valueOf("2026-04-02"),
              Timestamp.valueOf("2026-04-02 00:00:00")) ::
            Row(
              3,
              "plain",
              new java.math.BigDecimal("7.00"),
              null,
              Date.valueOf("2026-04-03"),
              Timestamp.valueOf("2026-04-03 23:59:59")) ::
            Nil
        )
    }
  }

  test("Paimon procedure: load_file jsonl with nested struct / array / map") {
    withTempDir {
      dir =>
        writeText(
          dir,
          "nested.jsonl",
          """{"id":1,"addr":{"city":"SH","zip":"200000"},"tags":["a","b"],"scores":{"math":90,"en":80}}""",
          """{"id":2,"addr":{"city":"BJ","zip":"100000"},"tags":[],"scores":{}}""",
          """{"id":3,"addr":null,"tags":["x"],"scores":{"math":70}}"""
        )

        spark.sql(s"""
                     |CREATE TABLE T (
                     |  id INT,
                     |  addr STRUCT<city: STRING, zip: STRING>,
                     |  tags ARRAY<STRING>,
                     |  scores MAP<STRING, INT>
                     |) USING PAIMON
                     |""".stripMargin)

        spark.sql(
          s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}', format => 'jsonl')")

        checkAnswer(
          spark.sql("SELECT id, addr.city, addr.zip, tags, scores['math'] FROM T ORDER BY id"),
          Row(1, "SH", "200000", Seq("a", "b"), 90) ::
            Row(2, "BJ", "100000", Seq.empty[String], null) ::
            Row(3, null, null, Seq("x"), 70) ::
            Nil
        )
    }
  }

  test("Paimon procedure: load_file jsonl FAILFAST opt-in aborts on malformed row") {
    withTempDir {
      dir =>
        writeText(
          dir,
          "bad.jsonl",
          """{"id":1,"name":"a"}""",
          """{"id":2,"name":""", // broken: unterminated
          """{"id":3,"name":"c"}""")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING)
                     |USING PAIMON
                     |""".stripMargin)

        val e = intercept[Exception] {
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'jsonl', options => map('mode','FAILFAST'))")
        }
        val msg = e.getMessage + " || " + rootCause(e)
        assert(
          msg.contains("Malformed") || msg.contains("FAILFAST") || msg.contains("malformed"),
          s"unexpected error: $msg")
    }
  }

  test("Paimon procedure: load_file jsonl default PERMISSIVE drops malformed row") {
    withTempDir {
      dir =>
        writeText(
          dir,
          "bad.jsonl",
          """{"id":1,"name":"a"}""",
          """{"id":2,"name":""", // broken
          """{"id":3,"name":"c"}""")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING)
                     |USING PAIMON
                     |""".stripMargin)

        // Default mode is PERMISSIVE. Bad row is isolated via _corrupt_record and dropped;
        // valid rows land in the table; counts reflect the split.
        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'jsonl')"),
          Row(true, 2L, 1L) :: Nil)

        checkAnswer(spark.sql("SELECT * FROM T ORDER BY id"), Row(1, "a") :: Row(3, "c") :: Nil)
    }
  }

  test("Paimon procedure: load_file csv uses \\x01 as default separator") {
    withTempDir {
      dir =>
        // No explicit sep in options — procedure should default to \x01.
        val sep = "\u0001"
        writeText(dir, "a.csv", s"id${sep}name", s"1${sep}a", s"2${sep}b")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING)
                     |USING PAIMON
                     |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'csv')"),
          Row(true, 2L, 0L) :: Nil)

        checkAnswer(spark.sql("SELECT * FROM T ORDER BY id"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("Paimon procedure: load_file jsonl recognises camelCase alias for snake_case column") {
    withTempDir {
      dir =>
        writeText(
          dir,
          "alias.jsonl",
          """{"id":1,"dataSource":"qt_v4"}""",
          """{"id":2,"data_source":"qt_v5"}""")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, data_source STRING)
                     |USING PAIMON
                     |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'jsonl')"),
          Row(true, 2L, 0L) :: Nil)

        checkAnswer(
          spark.sql("SELECT id, data_source FROM T ORDER BY id"),
          Row(1, "qt_v4") :: Row(2, "qt_v5") :: Nil)
    }
  }

  test("Paimon procedure: load_file jsonl prefers standard name when both aliases present") {
    withTempDir {
      dir =>
        // Both keys present in the same row — standard (table column) name must win.
        writeText(
          dir,
          "conflict.jsonl",
          """{"id":1,"data_source":"standard","dataSource":"variant"}""")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, data_source STRING)
                     |USING PAIMON
                     |""".stripMargin)

        spark.sql(
          s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
            s" format => 'jsonl')")

        checkAnswer(spark.sql("SELECT id, data_source FROM T"), Row(1, "standard") :: Nil)
    }
  }

  test("Paimon procedure: load_file csv recognises camelCase header for snake_case column") {
    withTempDir {
      dir =>
        val sep = "\u0001"
        writeText(dir, "alias.csv", s"id${sep}dataSource", s"1${sep}qt_v4", s"2${sep}qt_v5")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, data_source STRING)
                     |USING PAIMON
                     |""".stripMargin)

        spark.sql(
          s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
            s" format => 'csv')")

        checkAnswer(
          spark.sql("SELECT id, data_source FROM T ORDER BY id"),
          Row(1, "qt_v4") :: Row(2, "qt_v5") :: Nil)
    }
  }

  test("Paimon procedure: load_file csv with nested struct / array / map (JSON-in-cell)") {
    withTempDir {
      dir =>
        val sep = ""
        writeText(
          dir,
          "nested.csv",
          s"id${sep}addr${sep}tags${sep}scores",
          s"""1$sep{"city":"SH","zip":"200000"}$sep["a","b"]$sep{"math":90,"en":80}""",
          s"""2$sep{"city":"BJ","zip":"100000"}$sep[]$sep{}""",
          s"""3$sep$sep["x"]$sep{"math":70}"""
        )

        spark.sql(s"""
                     |CREATE TABLE T (
                     |  id INT,
                     |  addr STRUCT<city: STRING, zip: STRING>,
                     |  tags ARRAY<STRING>,
                     |  scores MAP<STRING, INT>
                     |) USING PAIMON
                     |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}', format => 'csv')"),
          Row(true, 3L, 0L) :: Nil
        )

        checkAnswer(
          spark.sql("SELECT id, addr.city, addr.zip, tags, scores['math'] FROM T ORDER BY id"),
          Row(1, "SH", "200000", Seq("a", "b"), 90) ::
            Row(2, "BJ", "100000", Seq.empty[String], null) ::
            Row(3, null, null, Seq("x"), 70) ::
            Nil
        )
    }
  }

  test("Paimon procedure: load_file csv with deeply nested map<string, struct<array<struct>>>") {
    withTempDir {
      dir =>
        val sep = ""
        val cell =
          """{"k1":{"version":"v1","items":[{"id":10,"key":"a"},{"id":11,"key":"b"}]},""" +
            """"k2":{"version":"v2","items":[]}}"""
        writeText(
          dir,
          "deep.csv",
          s"id${sep}data",
          s"1$sep$cell"
        )

        spark.sql(
          s"""
             |CREATE TABLE T (
             |  id INT,
             |  data MAP<STRING, STRUCT<version: STRING, items: ARRAY<STRUCT<id: BIGINT, `key`: STRING>>>>
             |) USING PAIMON
             |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}', format => 'csv')"),
          Row(true, 1L, 0L) :: Nil
        )

        checkAnswer(
          spark.sql(
            "SELECT id, data['k1'].version, data['k1'].items[0].id, data['k1'].items[0].key, " +
              "size(data['k2'].items) FROM T"),
          Row(1, "v1", 10L, "a", 0) :: Nil
        )
    }
  }

  test(
    "Paimon procedure: load_file csv malformed nested cell yields all-null struct, row still written") {
    withTempDir {
      dir =>
        val sep = ""
        writeText(
          dir,
          "partial.csv",
          s"id${sep}addr",
          s"""1$sep{"city":"SH","zip":"200000"}""",
          s"2${sep}not-a-json",
          s"""3$sep{"city":"BJ","zip":"100000"}"""
        )

        spark.sql(s"""
                     |CREATE TABLE T (id INT, addr STRUCT<city: STRING, zip: STRING>)
                     |USING PAIMON
                     |""".stripMargin)

        // All three rows are valid CSV records — invalid_count counts row-level failures only.
        // The malformed JSON cell on row 2 produces a struct with all fields null (PERMISSIVE
        // semantics of from_json); the row itself is still written.
        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}', format => 'csv')"),
          Row(true, 3L, 0L) :: Nil
        )

        checkAnswer(
          spark.sql("SELECT id, addr FROM T ORDER BY id"),
          Row(1, Row("SH", "200000")) ::
            Row(2, Row(null, null)) ::
            Row(3, Row("BJ", "100000")) ::
            Nil
        )
    }
  }

  test("Paimon procedure: load_file rejects unsupported format") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT)
                 |USING PAIMON
                 |""".stripMargin)
    val e = intercept[Exception] {
      spark.sql(
        "CALL paimon.sys.load_file(table => 'test.T', path => 'file:///tmp/x', format => 'parquet')")
    }
    assert(
      e.getMessage.contains("Unsupported format") || rootCause(e).contains("Unsupported format"))
  }

  test("Paimon procedure: load_file rejects CSV header=false") {
    withTempDir {
      dir =>
        writeText(dir, "a.csv", "1,a", "2,b")

        spark.sql(s"""
                     |CREATE TABLE T (id INT, name STRING)
                     |USING PAIMON
                     |""".stripMargin)

        val e = intercept[Exception] {
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'csv', options => map('header','false','sep',','))")
        }
        assert(
          e.getMessage.contains("header=true") || rootCause(e).contains("header=true"),
          s"unexpected error: ${e.getMessage}")
    }
  }

  test("Paimon procedure: load_file csv multiLine=true is the default (quoted newline cell)") {
    withTempDir {
      dir =>
        // A quoted cell containing a newline used to tear the row apart under the default
        // multiLine=false, which made every affected row malformed and (under PERMISSIVE)
        // silently dropped — the count agg was unaffected by pruning so it still reported
        // valid=N, invalid=0 while the write produced an empty commit (no snapshot).
        val f = new File(dir, "ml.csv")
        val w = new PrintWriter(f)
        try {
          w.println("id,note")
          w.println("1,\"line one\nline two\"")
          w.println("2,plain")
        } finally w.close()

        spark.sql(s"""
                     |CREATE TABLE T (id INT, note STRING)
                     |USING PAIMON
                     |""".stripMargin)

        // No multiLine in options — procedure must default it to true.
        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'csv', options => map('sep', ','))"),
          Row(true, 2L, 0L) :: Nil
        )

        // valid_count must equal the number of rows actually committed — i.e. the count agg
        // and the write path must agree on which rows are corrupt. Before the fix, count
        // pruning hid the malformed row from invalid_count and the write silently dropped it.
        checkAnswer(
          spark.sql("SELECT id, note FROM T ORDER BY id"),
          Row(1, "line one\nline two") :: Row(2, "plain") :: Nil)
    }
  }

  test("Paimon procedure: load_file csv count agg matches write under PERMISSIVE") {
    // Regression: before the fix, the count agg only referenced one probe column, so column
    // pruning made the parser skip type-checking other columns. PERMISSIVE then never marked
    // type-mismatch rows as corrupt at count time, but the write path (which referenced all
    // columns via the projection) did mark and drop them — producing valid_count > rows
    // committed and a 0-message commit.
    withTempDir {
      dir =>
        writeText(
          dir,
          "mixed.csv",
          "id,age",
          "1,42",
          "2,not-a-number", // bad row: only the second column fails to parse as INT
          "3,7"
        )

        spark.sql(s"""
                     |CREATE TABLE T (id INT, age INT)
                     |USING PAIMON
                     |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'csv', options => map('sep', ','))"),
          Row(true, 2L, 1L) :: Nil
        )

        checkAnswer(spark.sql("SELECT id, age FROM T ORDER BY id"), Row(1, 42) :: Row(3, 7) :: Nil)
    }
  }

  test("Paimon procedure: load_file csv default escape='\"' parses RFC4180 doubled quotes") {
    // Regression: Spark's CSV default escape is '\\', which mis-parses the very common
    // RFC 4180 pattern of a quoted JSON cell with doubled inner quotes — e.g. `"{""k"":1}"`.
    // Before defaulting escape to '"', every such row was torn apart and the whole file
    // came back as invalid. After the fix the rows parse and land in the table — the
    // exact in-cell representation depends on univocity's escape==quote semantics, which
    // we don't try to lock down here; we only assert that the file is no longer
    // wholesale-rejected.
    withTempDir {
      dir =>
        val q = "\""
        writeText(
          dir,
          "rfc4180.csv",
          "id,payload",
          "1," + q + "{" + q + q + "k" + q + q + ":" + q + q + "v1" + q + q + "}" + q,
          "2," + q + "{" + q + q + "k" + q + q + ":" + q + q + "v2" + q + q + "}" + q
        )

        spark.sql(s"""
                     |CREATE TABLE T (id INT, payload STRING)
                     |USING PAIMON
                     |""".stripMargin)

        // Only sep + multiLine overridden — escape must default to '"', not '\\'.
        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
              s" format => 'csv', options => map('sep', ',', 'multiLine', 'false'))"),
          Row(true, 2L, 0L) :: Nil
        )

        checkAnswer(spark.sql("SELECT count(*) FROM T"), Row(2L) :: Nil)
        checkAnswer(
          spark.sql("SELECT id FROM T ORDER BY id"),
          Row(1) :: Row(2) :: Nil
        )
    }
  }

  private def rootCause(t: Throwable): String = {
    var cur = t
    while (cur.getCause != null) cur = cur.getCause
    Option(cur.getMessage).getOrElse("")
  }
}
