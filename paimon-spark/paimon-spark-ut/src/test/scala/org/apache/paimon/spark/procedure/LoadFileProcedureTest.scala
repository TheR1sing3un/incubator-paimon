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
              s" format => 'csv', options => map('header', 'true'))"),
          Row(true) :: Nil
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
          Row(true) :: Nil)

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
            s" format => 'csv', options => map('header', 'true'))")

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
            s" format => 'csv', options => map('header','true','escape','$dq'))")

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

  test("Paimon procedure: load_file jsonl defaults to FAILFAST on malformed row") {
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
            s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}', format => 'jsonl')")
        }
        // FAILFAST surfaces as an analysis/runtime exception somewhere in the chain.
        assert(e != null)
    }
  }

  test("Paimon procedure: load_file jsonl PERMISSIVE override silently tolerates malformed row") {
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

        spark.sql(
          s"CALL paimon.sys.load_file(table => 'test.T', path => '${dir.toURI}'," +
            s" format => 'jsonl', options => map('mode','PERMISSIVE'))")

        // Good rows land; malformed row produces an all-null record under PERMISSIVE.
        val ids =
          spark.sql("SELECT id FROM T ORDER BY id NULLS FIRST").collect().map(_.get(0)).toSeq
        assert(ids.contains(1) && ids.contains(3))
    }
  }

  test("Paimon procedure: load_file csv rejects nested columns upfront") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, addr STRUCT<city: STRING>)
                 |USING PAIMON
                 |""".stripMargin)
    val e = intercept[Exception] {
      spark.sql(
        "CALL paimon.sys.load_file(table => 'test.T', path => 'file:///tmp/x', format => 'csv')")
    }
    val msg = e.getMessage + " | " + rootCause(e)
    assert(msg.contains("CSV format does not support nested types"))
    assert(msg.contains("addr"))
    assert(msg.contains("jsonl"))
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

  private def rootCause(t: Throwable): String = {
    var cur = t
    while (cur.getCause != null) cur = cur.getCause
    Option(cur.getMessage).getOrElse("")
  }
}
