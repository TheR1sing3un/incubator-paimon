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

package org.apache.paimon.spark.dataset;

import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.SimpleHttpClient;
import org.apache.paimon.spark.dataset.model.ConfigResponse;
import org.apache.paimon.spark.dataset.model.DatasetInfo;
import org.apache.paimon.spark.dataset.model.ListResponse;
import org.apache.paimon.spark.dataset.model.NamespaceInfo;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST client for the dataset-catalog service.
 *
 * <p>Uses Paimon's {@link SimpleHttpClient} for HTTP transport and Jackson (via {@link RESTApi})
 * for JSON serialization. Does not depend on kling-lakehouse code.
 *
 * <p>API endpoints:
 *
 * <ul>
 *   <li>GET /config — Paimon connection configuration
 *   <li>GET /namespaces — list all namespaces (returns {@code {"items": [...]}})
 *   <li>GET /namespaces/{ns}/datasets — list datasets in a namespace (returns {@code {"items":
 *       [...]}})
 *   <li>GET /namespaces/{ns}/datasets/byname/{name} — resolve dataset by name
 *   <li>GET /namespaces/{ns} — check namespace existence
 * </ul>
 */
public class DatasetRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetRestClient.class);

    /** Default context path for the dataset-catalog REST API. */
    public static final String DEFAULT_CONTEXT_PATH = "/api/v1";

    private static final TypeReference<ListResponse<NamespaceInfo>> NAMESPACE_LIST_TYPE =
            new TypeReference<ListResponse<NamespaceInfo>>() {};
    private static final TypeReference<ListResponse<DatasetInfo>> DATASET_LIST_TYPE =
            new TypeReference<ListResponse<DatasetInfo>>() {};

    private final String baseUrl;
    private final Map<String, String> headers;

    public DatasetRestClient(String baseUrl, String authToken, String userId) {
        this(baseUrl, DEFAULT_CONTEXT_PATH, authToken, userId);
    }

    public DatasetRestClient(String baseUrl, String contextPath, String authToken, String userId) {
        // Normalize: strip all trailing slashes from uri and context path
        String normalizedUri = stripTrailingSlashes(baseUrl);
        String normalizedPath = stripTrailingSlashes(contextPath);
        this.baseUrl = normalizedUri + normalizedPath;
        this.headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");
        if (authToken != null && !authToken.isEmpty()) {
            headers.put("Authorization", "Bearer " + authToken);
        }
        if (userId != null && !userId.isEmpty()) {
            headers.put("X-User-Id", userId);
        }
    }

    /** Fetches the Paimon connection configuration from the dataset-catalog. */
    public ConfigResponse getConfig() {
        String url = baseUrl + "/config";
        String json = doGet(url);
        return fromJson(json, ConfigResponse.class);
    }

    /** Lists all available namespaces. Server returns {@code {"items": [...]}}. */
    public List<NamespaceInfo> listNamespaces() {
        String url = baseUrl + "/namespaces";
        String json = doGet(url);
        return fromJsonList(json, NAMESPACE_LIST_TYPE).getItems();
    }

    /** Lists all datasets within the given namespace. Server returns {@code {"items": [...]}}. */
    public List<DatasetInfo> listDatasets(String namespace) {
        String url = baseUrl + "/namespaces/" + encode(namespace) + "/datasets";
        String json = doGet(url);
        return fromJsonList(json, DATASET_LIST_TYPE).getItems();
    }

    /** Resolves a dataset by its logical name within a namespace. */
    public DatasetInfo getDatasetByName(String namespace, String datasetName) {
        String url =
                baseUrl
                        + "/namespaces/"
                        + encode(namespace)
                        + "/datasets/byname/"
                        + encode(datasetName);
        String json = doGet(url);
        return fromJson(json, DatasetInfo.class);
    }

    /** Checks whether a namespace exists. */
    public boolean namespaceExists(String namespace) {
        try {
            String url = baseUrl + "/namespaces/" + encode(namespace);
            doGet(url);
            return true;
        } catch (Exception e) {
            LOG.debug("Namespace '{}' does not exist: {}", namespace, e.getMessage());
            return false;
        }
    }

    /** Checks whether a dataset exists within a namespace. */
    public boolean datasetExists(String namespace, String datasetName) {
        try {
            getDatasetByName(namespace, datasetName);
            return true;
        } catch (Exception e) {
            LOG.debug("Dataset '{}.{}' does not exist: {}", namespace, datasetName, e.getMessage());
            return false;
        }
    }

    private String doGet(String url) {
        try {
            return SimpleHttpClient.INSTANCE.get(url, null, headers);
        } catch (IOException e) {
            throw new RuntimeException("Failed to GET " + url + ": " + e.getMessage(), e);
        }
    }

    private static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return RESTApi.fromJson(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse JSON response as "
                            + clazz.getSimpleName()
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    private static <T> T fromJsonList(String json, TypeReference<T> typeRef) {
        try {
            return RESTApi.OBJECT_MAPPER.readValue(json, typeRef);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON list response: " + e.getMessage(), e);
        }
    }

    private static String encode(String value) {
        return RESTUtil.encodeString(value);
    }

    private static String stripTrailingSlashes(String s) {
        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }
}
