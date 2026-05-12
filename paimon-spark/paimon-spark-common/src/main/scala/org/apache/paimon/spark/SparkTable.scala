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

package org.apache.paimon.spark

import org.apache.paimon.spark.rowops.PaimonSparkCopyOnWriteOperation
import org.apache.paimon.table.{FileStoreTable, Table}

import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperationBuilder, RowLevelOperationInfo}

/**
 * A spark [[org.apache.spark.sql.connector.catalog.Table]] for paimon.
 *
 * This base variant does NOT implement [[SupportsRowLevelOperations]]. Spark's stock
 * `RewriteDeleteFromTable` / `RewriteUpdateTable` / `RewriteMergeIntoTable` rules sit in the
 * Resolution batch (fixedPoint) and fire BEFORE Paimon's own postHocResolutionRule interceptors
 * (`PaimonDeleteTable`, `PaimonUpdateTable`, `PaimonMergeInto`). If this base class implemented
 * `SupportsRowLevelOperations`, Spark would immediately call `newRowLevelOperationBuilder` on
 * tables whose V2 write is disabled (e.g. primary-key tables that fall back to V1 write) and fail
 * before Paimon has a chance to rewrite the plan to a V1 command. Likewise, deletion-vector,
 * row-tracking, and data-evolution tables need to stay on Paimon's V1 postHoc path even when
 * `useV2Write=true`, so they must also not expose `SupportsRowLevelOperations`.
 *
 * Tables that DO support V2 row-level operations use the [[SparkTableWithRowLevelOps]] subclass
 * instead; the [[SparkTable.of]] factory picks the right variant via
 * [[SparkTable.supportsV2RowLevelOps]], which is kept in lockstep with
 * `RowLevelHelper.shouldFallbackToV1`.
 */
case class SparkTable(override val table: Table) extends PaimonSparkTableBase(table)

/**
 * A Paimon [[SparkTable]] that additionally exposes V2 row-level operations to Spark. Constructed
 * by [[SparkTable.of]] when [[SparkTable.supportsV2RowLevelOps]] is true.
 */
class SparkTableWithRowLevelOps(tableArg: Table)
  extends SparkTable(tableArg)
  with SupportsRowLevelOperations {

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    table match {
      case t: FileStoreTable =>
        () => new PaimonSparkCopyOnWriteOperation(t, info)
      case _ =>
        throw new UnsupportedOperationException(
          "Row-level write operation is only supported for FileStoreTable. " +
            s"Actual table type: ${table.getClass.getSimpleName}")
    }
  }
}

/** Factory helpers for constructing the right [[SparkTable]] subclass. */
object SparkTable {

  /**
   * Returns a [[SparkTable]] variant suitable for the given table: [[SparkTableWithRowLevelOps]]
   * when the table can participate in Spark's V2 row-level ops path, the plain [[SparkTable]]
   * otherwise.
   */
  def of(table: Table): SparkTable = {
    // We need `useV2Write` (and other coreOptions) which are instance state, so construct once to
    // probe and promote to the row-level-ops variant only if the table truly supports the V2
    // row-level write path. The base instance is cheap and is discarded if we decide to return
    // the subclass.
    val base = SparkTable(table)
    if (supportsV2RowLevelOps(base)) new SparkTableWithRowLevelOps(table) else base
  }

  /**
   * Whether the given table supports Paimon's V2 row-level operations, i.e. whether it is safe to
   * expose [[SupportsRowLevelOperations]] to Spark.
   *
   * This must stay in sync with
   * `org.apache.paimon.spark.catalyst.analysis.RowLevelHelper#shouldFallbackToV1` — the two
   * predicates are logical complements. If they diverge, Spark's row-level rewrite rules will
   * intercept DML on tables that Paimon expects to handle through its postHoc V1 fallback, leaving
   * primary-key / deletion-vector / row-tracking / data-evolution tables with broken
   * MERGE/UPDATE/DELETE dispatch.
   *
   * Per-version shims for Spark 3.2/3.3/3.4 each ship their own
   * `org.apache.paimon.spark.SparkTable` (class + companion) that shadows this one at packaging
   * time. Those shim companions hard-code `false` because Spark < 3.5 cannot participate in V2
   * row-level ops anyway.
   */
  private[spark] def supportsV2RowLevelOps(sparkTable: SparkTable): Boolean = {
    if (org.apache.spark.SPARK_VERSION < "3.5") return false
    if (!sparkTable.useV2Write) return false
    sparkTable.getTable match {
      case fs: FileStoreTable =>
        fs.primaryKeys().isEmpty &&
        !sparkTable.coreOptions.deletionVectorsEnabled() &&
        !sparkTable.coreOptions.rowTrackingEnabled() &&
        !sparkTable.coreOptions.dataEvolutionEnabled()
      case _ => false
    }
  }
}

case class SparkIcebergTable(table: Table) extends BaseTable

case class SparkLanceTable(table: Table) extends BaseTable

case class SparkObjectTable(table: Table) extends BaseTable
