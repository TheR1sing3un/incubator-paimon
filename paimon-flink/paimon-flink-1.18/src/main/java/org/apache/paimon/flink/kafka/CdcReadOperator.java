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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Operator that reads {@link Split}s from the upstream {@link
 * org.apache.paimon.flink.source.operator.MonitorSource} and emits {@link CdcRecord}s.
 *
 * <p>This is based on {@link org.apache.paimon.flink.source.operator.ReadOperator} but outputs
 * schema-less {@link CdcRecord} instead of typed {@code RowData}, and supports schema evolution via
 * {@link SchemaEvolvingTableRead}.
 */
public class CdcReadOperator extends AbstractStreamOperator<CdcRecord>
        implements OneInputStreamOperator<Split, CdcRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(CdcReadOperator.class);
    private static final long serialVersionUID = 1L;

    private final SerializableSupplier<TableRead> readSupplier;
    private final List<DataField> initialFields;

    private transient TableRead read;
    private transient StreamRecord<CdcRecord> reuseRecord;
    private transient IOManager ioManager;
    private transient ObjectMapper objectMapper;
    private transient List<String> fieldNames;
    private transient List<DataType> fieldTypes;
    private transient InternalRow.FieldGetter[] fieldGetters;

    public CdcReadOperator(
            SerializableSupplier<TableRead> readSupplier, List<DataField> initialFields) {
        this.readSupplier = readSupplier;
        this.initialFields = initialFields;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        this.read = readSupplier.get().withIOManager(ioManager);

        // If the read is schema-evolving, register a listener to update field mappings
        if (read instanceof SchemaEvolvingTableRead) {
            ((SchemaEvolvingTableRead) read).setSchemaChangeListener(this::updateFieldMappings);
        }

        updateFieldMappings(initialFields);
        this.reuseRecord = new StreamRecord<>(null);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void processElement(StreamRecord<Split> record) throws Exception {
        Split split = record.getValue();
        try (CloseableIterator<InternalRow> iterator =
                read.createReader(split).toCloseableIterator()) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                reuseRecord.replace(convertToCdcRecord(row));
                output.collect(reuseRecord);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (read instanceof SchemaEvolvingTableRead) {
            ((SchemaEvolvingTableRead) read).close();
        }
        if (ioManager != null) {
            ioManager.close();
        }
    }

    void updateFieldMappings(List<DataField> fields) {
        this.fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        this.fieldTypes = fields.stream().map(DataField::type).collect(Collectors.toList());
        this.fieldGetters = InternalRowUtils.createFieldGetters(fieldTypes);
        LOG.info("Updated field mappings: {}", fieldNames);
    }

    private CdcRecord convertToCdcRecord(InternalRow row) {
        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < fieldGetters.length; i++) {
            Object value = fieldGetters[i].getFieldOrNull(row);
            if (value != null) {
                data.put(fieldNames.get(i), convertToString(value, fieldTypes.get(i)));
            }
        }
        return new CdcRecord(row.getRowKind(), data);
    }

    private String convertToString(Object value, DataType type) {
        switch (type.getTypeRoot()) {
            case DATE:
                return DateTimeUtils.formatDate((int) value);
            case TIME_WITHOUT_TIME_ZONE:
                int precision = ((TimeType) type).getPrecision();
                return DateTimeUtils.formatTimestampMillis((int) value, precision);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp ts = (Timestamp) value;
                return ts.toLocalDateTime().toString();
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString((byte[]) value);
            case ARRAY:
                return convertArrayToString((InternalArray) value, (ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return convertMapToString(
                        (InternalMap) value, mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return convertMapToString(
                        (InternalMap) value,
                        multisetType.getElementType(),
                        new org.apache.paimon.types.IntType());
            case ROW:
                return convertRowToString((InternalRow) value, (RowType) type);
            default:
                return value.toString();
        }
    }

    private String convertArrayToString(InternalArray array, ArrayType arrayType) {
        DataType elementType = arrayType.getElementType();
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        List<Object> list = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            list.add(element == null ? null : convertToString(element, elementType));
        }
        try {
            return objectMapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            return list.toString();
        }
    }

    private String convertMapToString(InternalMap map, DataType keyType, DataType valueType) {
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < keyArray.size(); i++) {
            Object k = keyGetter.getElementOrNull(keyArray, i);
            Object v = valueGetter.getElementOrNull(valueArray, i);
            result.put(
                    k == null ? "null" : convertToString(k, keyType),
                    v == null ? null : convertToString(v, valueType));
        }
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            return result.toString();
        }
    }

    private String convertRowToString(InternalRow row, RowType rowType) {
        List<DataField> fields = rowType.getFields();
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            InternalRow.FieldGetter getter =
                    InternalRowUtils.createNullCheckingFieldGetter(field.type(), i);
            Object v = getter.getFieldOrNull(row);
            if (v != null) {
                result.put(field.name(), convertToString(v, field.type()));
            }
        }
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            return result.toString();
        }
    }
}
