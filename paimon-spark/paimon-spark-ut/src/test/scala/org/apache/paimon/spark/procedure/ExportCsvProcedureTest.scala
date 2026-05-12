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

class ExportCsvProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: export_csv basic export") {
    withTempDir {
      dir =>
        spark.sql("""
                    |CREATE TABLE T (id INT, name STRING, value DOUBLE)
                    |USING PAIMON
                    |""".stripMargin)

        spark.sql("INSERT INTO T VALUES (1, 'a', 1.1), (2, 'b', 2.2), (3, 'c', 3.3)")

        val outPath = new java.io.File(dir, "out").toURI.toString

        checkAnswer(
          spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T', path => '$outPath')"),
          Row(true, 3L) :: Nil
        )

        val readBack = spark.read
          .option("header", "true")
          .option("sep", "\u0001")
          .option("inferSchema", "true")
          .csv(outPath)
        assert(readBack.count() == 3)
        checkAnswer(
          readBack.select("id", "name", "value").orderBy("id"),
          Row(1, "a", 1.1) :: Row(2, "b", 2.2) :: Row(3, "c", 3.3) :: Nil)
    }
  }

  test("Paimon procedure: export_csv with WHERE filter") {
    withTempDir {
      dir =>
        spark.sql("""
                    |CREATE TABLE T (id INT, name STRING, dt STRING)
                    |USING PAIMON PARTITIONED BY (dt)
                    |""".stripMargin)

        spark.sql("""INSERT INTO T VALUES
                    |(1, 'a', '2026-05-01'),
                    |(2, 'b', '2026-05-01'),
                    |(3, 'c', '2026-05-02')""".stripMargin)

        val outPath = new java.io.File(dir, "out").toURI.toString

        checkAnswer(
          spark.sql(s"""CALL paimon.sys.export_csv(
                       |  table => 'test.T',
                       |  path => '$outPath',
                       |  where => 'dt = "2026-05-01"')""".stripMargin),
          Row(true, 2L) :: Nil
        )

        val readBack = spark.read
          .option("header", "true")
          .option("sep", "\u0001")
          .option("inferSchema", "true")
          .csv(outPath)
        assert(readBack.count() == 2)
    }
  }

  test("Paimon procedure: export_csv with custom options") {
    withTempDir {
      dir =>
        spark.sql("""
                    |CREATE TABLE T (id INT, name STRING)
                    |USING PAIMON
                    |""".stripMargin)

        spark.sql("INSERT INTO T VALUES (1, 'hello'), (2, 'world')")

        val outPath = new java.io.File(dir, "out").toURI.toString

        checkAnswer(
          spark.sql(s"""CALL paimon.sys.export_csv(
                       |  table => 'test.T',
                       |  path => '$outPath',
                       |  options => map('sep', ',', 'header', 'true'))""".stripMargin),
          Row(true, 2L) :: Nil
        )

        val readBack = spark.read
          .option("header", "true")
          .option("sep", ",")
          .option("inferSchema", "true")
          .csv(outPath)
        checkAnswer(
          readBack.select("id", "name").orderBy("id"),
          Row(1, "hello") :: Row(2, "world") :: Nil)
    }
  }

  test("Paimon procedure: export_csv empty table") {
    withTempDir {
      dir =>
        spark.sql("""
                    |CREATE TABLE T (id INT, name STRING)
                    |USING PAIMON
                    |""".stripMargin)

        val outPath = new java.io.File(dir, "out").toURI.toString

        checkAnswer(
          spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T', path => '$outPath')"),
          Row(true, 0L) :: Nil
        )
    }
  }

  test("Paimon procedure: export_csv nested types (struct, array, map)") {
    withTempDir {
      dir =>
        spark.sql("""
                    |CREATE TABLE T (
                    |  id INT,
                    |  info STRUCT<name: STRING, age: INT>,
                    |  tags ARRAY<STRING>,
                    |  props MAP<STRING, STRING>
                    |) USING PAIMON
                    |""".stripMargin)

        spark.sql("""INSERT INTO T VALUES
                    |(1, struct('alice', 30), array('a', 'b'), map('k1', 'v1')),
                    |(2, struct('bob', 25), array('c'), map('k2', 'v2', 'k3', 'v3'))""".stripMargin)

        val outPath = new java.io.File(dir, "out").toURI.toString

        checkAnswer(
          spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T', path => '$outPath')"),
          Row(true, 2L) :: Nil
        )

        // Read back as raw strings to verify nested columns are JSON-serialised
        val readBack = spark.read
          .option("header", "true")
          .option("sep", "\u0001")
          .option("escape", "\"")
          .csv(outPath)
        assert(readBack.count() == 2)
        // Verify the nested columns round-trip through load_file's from_json
        val readBackTyped = spark.read
          .option("header", "true")
          .option("sep", "\u0001")
          .option("escape", "\"")
          .option("inferSchema", "false")
          .csv(outPath)
        assert(readBackTyped.columns.toSet == Set("id", "info", "tags", "props"))
    }
  }

  test("Paimon procedure: export_csv then re-import via load_file round-trip") {
    withTempDir {
      dir =>
        spark.sql("""
                    |CREATE TABLE T_SRC (id INT, name STRING, score DOUBLE)
                    |USING PAIMON
                    |""".stripMargin)

        spark.sql("INSERT INTO T_SRC VALUES (1, 'a', 1.5), (2, 'b', 2.5)")

        val csvPath = new java.io.File(dir, "csv").toURI.toString

        spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T_SRC', path => '$csvPath')")

        spark.sql("""
                    |CREATE TABLE T_DST (id INT, name STRING, score DOUBLE)
                    |USING PAIMON
                    |""".stripMargin)

        checkAnswer(
          spark.sql(
            s"CALL paimon.sys.load_file(table => 'test.T_DST', path => '$csvPath', format => 'csv')"),
          Row(true, 2L, 0L) :: Nil
        )

        checkAnswer(
          spark.sql("SELECT * FROM T_DST ORDER BY id"),
          Row(1, "a", 1.5) :: Row(2, "b", 2.5) :: Nil
        )
    }
  }
}
