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

import org.apache.paimon.table.Table

/** A spark [[org.apache.spark.sql.connector.catalog.Table]] for paimon. */
case class SparkTable(override val table: Table) extends PaimonSparkTableBase(table) {}

/**
 * Per-version shim companion. Spark < 3.5 cannot participate in V2 row-level ops, so the factory
 * always returns the plain base class. Mirrors the signature of the common module's `SparkTable.of`
 * so that shaded common bytecode calling `SparkTable.of(table)` resolves at runtime (otherwise
 * NoSuchMethodError on first DML statement).
 */
object SparkTable {
  def of(table: Table): SparkTable = SparkTable(table)
}
