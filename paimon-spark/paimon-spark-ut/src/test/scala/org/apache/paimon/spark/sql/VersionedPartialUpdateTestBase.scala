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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

/** Spark IT tests for the versioned-partial-update merge engine. */
abstract class VersionedPartialUpdateTestBase extends PaimonSparkTestBase {

  private def createTable(): Unit = {
    spark.sql("""
                |CREATE TABLE T (
                |  pk INT,
                |  single_col STRING,
                |  mv_col STRUCT<latest_version: STRING, latest_value: STRING,
                |                all_versioned_values: MAP<STRING, STRING>>
                |) TBLPROPERTIES (
                |  'primary-key' = 'pk',
                |  'bucket' = '1',
                |  'merge-engine' = 'versioned-partial-update',
                |  'versioned-partial-update.multi-version-fields' = 'mv_col',
                |  'deletion-vectors.enabled' = 'true',
                |  'sequence.snapshot-ordering' = 'true'
                |)
                |""".stripMargin)
  }

  private def insertUpsert(values: String): Unit = {
    withSparkSQLConf("spark.paimon.versioned-partial-update.merge-mode" -> "upsert") {
      spark.sql(s"INSERT INTO T VALUES $values")
    }
  }

  private def insertIgnore(values: String): Unit = {
    withSparkSQLConf("spark.paimon.versioned-partial-update.merge-mode" -> "ignore") {
      spark.sql(s"INSERT INTO T VALUES $values")
    }
  }

  /** Helper: a SQL value expression for a row with a single mv version entry. */
  private def row(pk: Int, singleCol: String, version: String, value: String): String = {
    val sc = if (singleCol == null) "NULL" else s"'$singleCol'"
    if (version == null) {
      s"($pk, $sc, NULL)"
    } else {
      s"($pk, $sc, STRUCT('$version', '$value', MAP('$version', '$value')))"
    }
  }

  /** Helper: expected Row for checkAnswer with a single mv version entry. */
  private def expected(
      pk: Int,
      singleCol: String,
      latestVer: String,
      latestVal: String,
      versions: Map[String, String]): Row = {
    Row(pk, singleCol, Row(latestVer, latestVal, versions))
  }

  // ===== Basic upsert tests =====

  test("versioned partial update: upsert write and read") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "hello"))
      insertUpsert(row(1, "B", "v2", "world"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "B", "v2", "world", Map("v1" -> "hello", "v2" -> "world")) :: Nil)
    }
  }

  test("versioned partial update: upsert overwrites existing version key") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "original"))
      insertUpsert(row(1, "B", "v1", "updated"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "B", "v1", "updated", Map("v1" -> "updated")) :: Nil)
    }
  }

  test("versioned partial update: multiple PKs") {
    withTable("T") {
      createTable()
      insertUpsert(s"${row(1, "A", "v1", "hello")}, ${row(2, "X", "v1", "world")}")

      checkAnswer(
        spark.sql("SELECT * FROM T ORDER BY pk"),
        expected(1, "A", "v1", "hello", Map("v1" -> "hello"))
          :: expected(2, "X", "v1", "world", Map("v1" -> "world"))
          :: Nil
      )
    }
  }

  // ===== Ignore mode tests =====

  test("versioned partial update: ignore does not overwrite single version column") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "hello"))
      insertIgnore(row(1, "B", "v2", "world"))

      // single_col stays "A", but v2 is appended (new key)
      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "A", "v2", "world", Map("v1" -> "hello", "v2" -> "world")) :: Nil)
    }
  }

  test("versioned partial update: ignore does not overwrite existing version key") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "original"))
      insertIgnore(row(1, "B", "v1", "updated"))

      // single_col stays "A", v1 stays "original"
      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "A", "v1", "original", Map("v1" -> "original")) :: Nil)
    }
  }

  test("versioned partial update: ignore fills null single version column") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, null, "v1", "hello"))
      insertIgnore(row(1, "filled", "v2", "world"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "filled", "v2", "world", Map("v1" -> "hello", "v2" -> "world")) :: Nil)
    }
  }

  test("versioned partial update: ignore adds new version key") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "hello"))
      insertIgnore(row(1, "B", "v2", "world"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "A", "v2", "world", Map("v1" -> "hello", "v2" -> "world")) :: Nil)
    }
  }

  // ===== Mixed mode: upsert after ignore =====

  test("versioned partial update: upsert overwrites after ignore") {
    withTable("T") {
      createTable()
      insertIgnore(row(1, "from_ignore", "v1", "ival"))
      insertUpsert(row(1, "from_upsert", "v1", "uval"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "from_upsert", "v1", "uval", Map("v1" -> "uval")) :: Nil)
    }
  }

  // ===== Three round: upsert -> ignore -> upsert =====

  test("versioned partial update: three round upsert-ignore-upsert") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "a_val"))
      insertIgnore(row(1, "B", "v2", "b_val"))
      insertUpsert(row(1, "C", "v1", "c_val"))

      // single_col: A -> ignore B -> upsert C = "C"
      // v1: a_val -> ignore keeps -> upsert c_val = "c_val"
      // v2: new from ignore = "b_val"
      // latest: v2 > v1 lexicographically
      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "C", "v2", "b_val", Map("v1" -> "c_val", "v2" -> "b_val")) :: Nil)
    }
  }

  // ===== Compaction preserves all version keys =====

  test("versioned partial update: 5 rounds of upsert preserve all version keys") {
    withTable("T") {
      createTable()
      for (i <- 1 to 5) {
        insertUpsert(row(1, s"val$i", s"v$i", s"data$i"))
      }

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(
          1,
          "val5",
          "v5",
          "data5",
          Map("v1" -> "data1", "v2" -> "data2", "v3" -> "data3", "v4" -> "data4", "v5" -> "data5")
        ) :: Nil
      )
    }
  }

  // ===== New PK with ignore mode =====

  test("versioned partial update: new PK with ignore mode sets values") {
    withTable("T") {
      createTable()
      insertIgnore(row(1, "hello", "v1", "world"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "hello", "v1", "world", Map("v1" -> "world")) :: Nil)
    }
  }

  // ===== Latest version is lexicographically greatest =====

  test("versioned partial update: latest version is lexicographic max") {
    withTable("T") {
      createTable()
      // v10 < v2 < v9 lexicographically
      insertUpsert(row(1, "A", "v10", "d10"))
      insertUpsert(row(1, "B", "v2", "d2"))
      insertUpsert(row(1, "C", "v9", "d9"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "C", "v9", "d9", Map("v10" -> "d10", "v2" -> "d2", "v9" -> "d9")) :: Nil)
    }
  }

  // ===== Multi-PK concurrent jobs =====

  test("versioned partial update: two jobs write different PKs") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "hello"))
      insertIgnore(row(2, "X", "v1", "world"))

      checkAnswer(
        spark.sql("SELECT * FROM T ORDER BY pk"),
        expected(1, "A", "v1", "hello", Map("v1" -> "hello"))
          :: expected(2, "X", "v1", "world", Map("v1" -> "world"))
          :: Nil
      )
    }
  }

  test("versioned partial update: two jobs write same PK different version keys") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "A", "v1", "from_a"))
      insertIgnore(row(1, "B", "v2", "from_b"))

      // single_col stays "A" (ignore), both version keys present
      checkAnswer(
        spark.sql("SELECT * FROM T"),
        expected(1, "A", "v2", "from_b", Map("v1" -> "from_a", "v2" -> "from_b")) :: Nil)
    }
  }

  // ===== Write only partial columns =====

  test("versioned partial update: write only mv column") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, null, "v1", "hello"))

      checkAnswer(
        spark.sql("SELECT * FROM T"),
        Row(1, null, Row("v1", "hello", Map("v1" -> "hello"))) :: Nil)
    }
  }

  test("versioned partial update: write only single version column") {
    withTable("T") {
      createTable()
      insertUpsert(row(1, "hello", null, null))

      checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "hello", null) :: Nil)
    }
  }

  // ===== Validation =====

  test("versioned partial update: rejects table without DV enabled") {
    withTable("T") {
      val e = intercept[Exception] {
        spark.sql("""
                    |CREATE TABLE T (
                    |  pk INT,
                    |  single_col STRING,
                    |  mv_col STRUCT<latest_version: STRING, latest_value: STRING,
                    |                all_versioned_values: MAP<STRING, STRING>>
                    |) TBLPROPERTIES (
                    |  'primary-key' = 'pk',
                    |  'bucket' = '1',
                    |  'merge-engine' = 'versioned-partial-update',
                    |  'versioned-partial-update.multi-version-fields' = 'mv_col',
                    |  'deletion-vectors.enabled' = 'false'
                    |)
                    |""".stripMargin)
      }
      assert(e.getMessage.contains("deletion-vectors.enabled = true"))
    }
  }
}
