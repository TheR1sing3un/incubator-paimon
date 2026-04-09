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

package org.apache.paimon.spark.dataset.model;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Namespace metadata returned by the dataset-catalog REST API.
 *
 * <p>Server response example: {@code {"namespace":"my_ns"}}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NamespaceInfo {

    private final String name;

    @JsonCreator
    public NamespaceInfo(@JsonProperty("namespace") String name) {
        this.name = name;
    }

    @JsonProperty("namespace")
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "NamespaceInfo{name='" + name + "'}";
    }
}
