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
import org.apache.paimon.spark.dataset.model.ConfigResponse;
import org.apache.paimon.spark.dataset.model.DatasetInfo;
import org.apache.paimon.spark.dataset.model.NamespaceInfo;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DatasetRestClient} using OkHttp MockWebServer. */
class DatasetRestClientTest {

    private MockWebServer server;
    private DatasetRestClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        client = new DatasetRestClient(server.url("/").toString(), "test-token", "test-user");
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void getConfigParsesResponse() throws Exception {
        ConfigResponse expected = new ConfigResponse("http://paimon:8080", "/warehouse");
        server.enqueue(
                new MockResponse()
                        .setBody(RESTApi.toJson(expected))
                        .addHeader("Content-Type", "application/json"));

        ConfigResponse result = client.getConfig();

        assertThat(result.getPaimonRestUri()).isEqualTo("http://paimon:8080");
        assertThat(result.getPaimonRestWarehouse()).isEqualTo("/warehouse");

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).isEqualTo("/api/v1/config");
        assertThat(req.getMethod()).isEqualTo("GET");
    }

    @Test
    void getConfigSendsAuthHeaders() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"paimon_rest_uri\":\"http://x\",\"paimon_rest_warehouse\":null}")
                        .addHeader("Content-Type", "application/json"));

        client.getConfig();

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("Authorization")).isEqualTo("Bearer test-token");
        assertThat(req.getHeader("X-User-Id")).isEqualTo("test-user");
    }

    @Test
    void listNamespaces() throws Exception {
        String json = "{\"items\":[{\"namespace\":\"ns1\"},{\"namespace\":\"ns2\"}]}";
        server.enqueue(
                new MockResponse().setBody(json).addHeader("Content-Type", "application/json"));

        List<NamespaceInfo> result = client.listNamespaces();

        assertThat(result).hasSize(2);
        assertThat(result.get(0).getName()).isEqualTo("ns1");
        assertThat(result.get(1).getName()).isEqualTo("ns2");

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).isEqualTo("/api/v1/namespaces");
    }

    @Test
    void listDatasets() throws Exception {
        String json =
                "{\"items\":[{\"name\":\"ds1\","
                        + "\"database_name\":\"db\",\"table_name\":\"t1\"}]}";
        server.enqueue(
                new MockResponse().setBody(json).addHeader("Content-Type", "application/json"));

        List<DatasetInfo> result = client.listDatasets("ns");

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getDatasetName()).isEqualTo("ds1");
        assertThat(result.get(0).getDatabaseName()).isEqualTo("db");
        assertThat(result.get(0).getTableName()).isEqualTo("t1");

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).isEqualTo("/api/v1/namespaces/ns/datasets");
    }

    @Test
    void getDatasetByName() throws Exception {
        DatasetInfo expected = new DatasetInfo("my_ds", "paimon_db", "tbl_42");
        server.enqueue(
                new MockResponse()
                        .setBody(RESTApi.toJson(expected))
                        .addHeader("Content-Type", "application/json"));

        DatasetInfo result = client.getDatasetByName("ns", "my_ds");

        assertThat(result.getDatasetName()).isEqualTo("my_ds");
        assertThat(result.getDatabaseName()).isEqualTo("paimon_db");
        assertThat(result.getTableName()).isEqualTo("tbl_42");

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).isEqualTo("/api/v1/namespaces/ns/datasets/byname/my_ds");
    }

    @Test
    void namespaceExistsReturnsTrueOn200() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("{\"namespace\":\"ns\"}")
                        .addHeader("Content-Type", "application/json"));

        assertThat(client.namespaceExists("ns")).isTrue();
    }

    @Test
    void namespaceExistsReturnsFalseOnError() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(404).setBody("Not Found"));

        assertThat(client.namespaceExists("missing")).isFalse();
    }

    @Test
    void datasetExistsReturnsTrueWhenFound() throws Exception {
        DatasetInfo info = new DatasetInfo("ds", "db", "t");
        server.enqueue(
                new MockResponse()
                        .setBody(RESTApi.toJson(info))
                        .addHeader("Content-Type", "application/json"));

        assertThat(client.datasetExists("ns", "ds")).isTrue();
    }

    @Test
    void datasetExistsReturnsFalseOnError() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(404).setBody("Not Found"));

        assertThat(client.datasetExists("ns", "missing")).isFalse();
    }

    @Test
    void serverErrorThrowsRuntimeException() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("Internal Server Error"));

        assertThatThrownBy(() -> client.getConfig()).isInstanceOf(RuntimeException.class);
    }

    @Test
    void noAuthTokenOmitsHeader() throws Exception {
        DatasetRestClient noAuthClient = new DatasetRestClient(server.url("/").toString(), "", "");
        server.enqueue(
                new MockResponse()
                        .setBody("{\"paimon_rest_uri\":\"http://x\"}")
                        .addHeader("Content-Type", "application/json"));

        noAuthClient.getConfig();

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("Authorization")).isNull();
        assertThat(req.getHeader("X-User-Id")).isNull();
    }

    // ======================== context-path ========================

    @Test
    void customContextPathUsedInRequests() throws Exception {
        DatasetRestClient customClient =
                new DatasetRestClient(server.url("/").toString(), "/custom/v2", "", "");
        server.enqueue(
                new MockResponse()
                        .setBody("{\"items\":[{\"namespace\":\"ns1\"}]}")
                        .addHeader("Content-Type", "application/json"));

        customClient.listNamespaces();

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).isEqualTo("/custom/v2/namespaces");
    }

    @Test
    void trailingSlashNormalized() throws Exception {
        DatasetRestClient slashClient =
                new DatasetRestClient(server.url("/").toString() + "/", "/api/v1/", "", "");
        server.enqueue(
                new MockResponse()
                        .setBody("{\"items\":[{\"namespace\":\"ns1\"}]}")
                        .addHeader("Content-Type", "application/json"));

        slashClient.listNamespaces();

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).isEqualTo("/api/v1/namespaces");
    }

    @Test
    void defaultContextPathIsApiV1() throws Exception {
        DatasetRestClient defaultClient =
                new DatasetRestClient(server.url("/").toString(), "token", "user");
        server.enqueue(
                new MockResponse()
                        .setBody("{\"paimon_rest_uri\":\"http://x\"}")
                        .addHeader("Content-Type", "application/json"));

        defaultClient.getConfig();

        RecordedRequest req = server.takeRequest();
        assertThat(req.getPath()).isEqualTo("/api/v1/config");
    }
}
