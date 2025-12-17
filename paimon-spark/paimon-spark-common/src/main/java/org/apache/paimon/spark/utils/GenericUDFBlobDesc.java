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

package org.apache.paimon.spark.utils;

import org.apache.paimon.data.BlobDescriptor;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

/** GenericUDFBlobDesc. */
@Description(
        name = "blobDesc",
        value = "_FUNC_(str, length) - Returns the binary of blobFile's desc",
        extended =
                "Example:\n"
                        + "  > SELECT _FUNC_('file:///local/path/to/blobFile', 100) FROM src LIMIT 1;\n ")
public class GenericUDFBlobDesc extends GenericUDF {
    private final BytesWritable result = new BytesWritable();
    private transient PrimitiveObjectInspectorConverter.StringConverter stringConverter;
    private transient PrimitiveObjectInspectorConverter.LongConverter longConverter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function blob_desc(s, length) takes exactly 2 arguments.");
        }

        ObjectInspector outputOI = null;
        stringConverter =
                new PrimitiveObjectInspectorConverter.StringConverter(
                        (PrimitiveObjectInspector) arguments[0]);

        longConverter =
                new PrimitiveObjectInspectorConverter.LongConverter(
                        (PrimitiveObjectInspector) arguments[1],
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        outputOI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        return outputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        byte[] data = null;

        String uri = null;
        if (arguments[0] != null) {
            uri = (String) stringConverter.convert(arguments[0].get());
        }
        if (uri == null) {
            return null;
        }

        org.apache.hadoop.io.LongWritable length = null;
        if (arguments[1] != null) {
            length = (org.apache.hadoop.io.LongWritable) longConverter.convert(arguments[1].get());
        }
        if (length == null) {
            return null;
        }

        data = new BlobDescriptor(uri, 0, length.get()).serialize();
        if (data == null) {
            return null;
        }

        result.set(new BytesWritable(data));
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("blob_desc", children);
    }
}
