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

package org.apache.paimon.spark.procedure;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Transforms incoming data to match Paimon dataset schema.
 *
 * <p>Multi-version fields are auto-detected from the target table schema. A field is considered
 * multi-version if its type matches:
 *
 * <pre>
 *   struct&lt;
 *     latest_version: string,
 *     latest_value:   &lt;original type&gt;,
 *     all_versioned_values: map&lt;string, &lt;original type&gt;&gt;
 *   &gt;
 * </pre>
 *
 * <p>Regular fields are passed through unchanged.
 */
public class SchemaTransformer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaTransformer.class);
    private static final String DEFAULT_VERSION = "v1.0.0";

    private final List<String> multiVersionFields;
    private final String versionValue;
    private final StructType tableSchema;

    public SchemaTransformer(StructType tableSchema, String versionValue) {
        this.tableSchema = tableSchema;
        this.multiVersionFields = detectMultiVersionFields(tableSchema);
        this.versionValue =
                (versionValue != null && !versionValue.isEmpty()) ? versionValue : DEFAULT_VERSION;
        LOG.info(
                "SchemaTransformer initialized: {} multi-version fields detected: {}, "
                        + "version value: {}",
                multiVersionFields.size(),
                multiVersionFields,
                this.versionValue);
    }

    /**
     * Transform a batch of data rows. For multi-version fields, wraps raw values into versioned
     * struct format using the configured version value as constant.
     *
     * @param df DataFrame with raw data columns
     * @return transformed DataFrame ready for Paimon write
     */
    public Dataset<Row> transform(Dataset<Row> df) {
        if (multiVersionFields.isEmpty()) {
            return df;
        }

        Column versionCol = functions.lit(versionValue);
        Dataset<Row> result = df;
        for (String fieldName : multiVersionFields) {
            DataType targetType = tableSchema.apply(fieldName).dataType();
            DataType expectedValueType = getLatestValueType(targetType);
            // codeflicker-fix: Issue-001/wadz6bmbi45oo4caoa9l
            if (expectedValueType != null && Arrays.asList(result.columns()).contains(fieldName)) {
                Column castedCol = result.col(fieldName).cast(expectedValueType);
                result = result.withColumn(fieldName, wrapVersionedField(castedCol, versionCol));
            } else {
                result = result.withColumn(fieldName, functions.lit(null).cast(targetType));
            }
        }
        return result;
    }

    /** Extract the latest_value DataType from a multi-version struct type. */
    private static DataType getLatestValueType(DataType multiVersionType) {
        if (multiVersionType instanceof StructType) {
            StructType st = (StructType) multiVersionType;
            if (st.fields().length >= 2 && st.fields()[1].name().equals("latest_value")) {
                return st.fields()[1].dataType();
            }
        }
        return null;
    }

    /**
     * Wrap a single field value into the versioned struct format.
     *
     * @param castedFieldCol the casted field column
     * @param versionCol the version key column
     * @return the wrapped struct column
     */
    public static Column wrapVersionedField(Column castedFieldCol, Column versionCol) {
        return functions.struct(
                versionCol.cast(DataTypes.StringType).as("latest_version"),
                castedFieldCol.as("latest_value"),
                functions
                        .map(versionCol.cast(DataTypes.StringType), castedFieldCol)
                        .as("all_versioned_values"));
    }

    public boolean hasMultiVersionFields() {
        return !multiVersionFields.isEmpty();
    }

    public List<String> getMultiVersionFields() {
        return multiVersionFields;
    }

    /** Detect multi-version fields by inspecting the target table schema types. */
    private static List<String> detectMultiVersionFields(StructType tableSchema) {
        List<String> result = new ArrayList<>();
        for (StructField field : tableSchema.fields()) {
            if (isMultiVersionType(field.dataType())) {
                result.add(field.name());
            }
        }
        return Collections.unmodifiableList(result);
    }

    /** Check if a DataType matches the multi-version struct pattern. */
    static boolean isMultiVersionType(DataType dt) {
        if (!(dt instanceof StructType)) {
            return false;
        }
        StructType st = (StructType) dt;
        if (st.fields().length != 3) {
            return false;
        }

        StructField[] fields = st.fields();
        if (!fields[0].name().equals("latest_version")) {
            return false;
        }
        if (!fields[1].name().equals("latest_value")) {
            return false;
        }
        if (!fields[2].name().equals("all_versioned_values")) {
            return false;
        }

        // latest_version must be StringType
        if (!fields[0].dataType().equals(DataTypes.StringType)) {
            return false;
        }

        // all_versioned_values must be MapType with StringType key
        if (!(fields[2].dataType() instanceof MapType)) {
            return false;
        }
        MapType mapType = (MapType) fields[2].dataType();
        if (!mapType.keyType().equals(DataTypes.StringType)) {
            return false;
        }

        // latest_value type and map value type should be the same
        return fields[1].dataType().equals(mapType.valueType());
    }
}
