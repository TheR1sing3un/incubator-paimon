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

package org.apache.paimon.rest.server;

import org.apache.paimon.options.Options;

import com.kuaishou.kling.lakehouse.metrics.MetricsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RESTCatalogServerConfigTest {

    @TempDir Path tempDir;

    @Test
    void testLoadConfigFile() throws Exception {
        Path configFile = tempDir.resolve("server.properties");
        Properties props = new Properties();
        props.setProperty("warehouse", "/tmp/warehouse");
        props.setProperty("rest-server.port", "9090");
        props.setProperty("rest-server.host", "localhost");
        try (FileOutputStream fos = new FileOutputStream(configFile.toFile())) {
            props.store(fos, null);
        }

        Map<String, String> map = RESTCatalogServer.loadConfigFile(configFile.toString());

        assertThat(map.get("warehouse")).isEqualTo("/tmp/warehouse");
        assertThat(map.get("rest-server.port")).isEqualTo("9090");
        assertThat(map.get("rest-server.host")).isEqualTo("localhost");
    }

    @Test
    void testLoadConfigFileNotFound() {
        assertThatThrownBy(() -> RESTCatalogServer.loadConfigFile("/nonexistent/path.properties"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to load config file");
    }

    @Test
    void testLoadMetricsConfig() throws Exception {
        Path configFile = tempDir.resolve("metrics-test.properties");
        Properties props = new Properties();
        props.setProperty("warehouse", "/tmp/warehouse");
        props.setProperty("metrics.service", "test-service");
        props.setProperty("metrics.cluster", "test-cluster");
        props.setProperty("metrics.namespace", "test.namespace");
        props.setProperty("metrics.deploy-group", "canary");
        props.setProperty("metrics.conf-version", "v1");
        props.setProperty("metrics.caller-registry", "app1,app2,app3");
        try (FileOutputStream fos = new FileOutputStream(configFile.toFile())) {
            props.store(fos, null);
        }

        Map<String, String> map = RESTCatalogServer.loadConfigFile(configFile.toString());
        Options options = new Options(map);

        assertThat(options.get(RESTCatalogServerOptions.METRICS_SERVICE)).isEqualTo("test-service");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_CLUSTER)).isEqualTo("test-cluster");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_NAMESPACE))
                .isEqualTo("test.namespace");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_DEPLOY_GROUP)).isEqualTo("canary");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_CONF_VERSION)).isEqualTo("v1");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_CALLER_REGISTRY))
                .isEqualTo("app1,app2,app3");
    }

    @Test
    void testMetricsConfigDefaults() {
        Options options = new Options();
        assertThat(options.get(RESTCatalogServerOptions.METRICS_SERVICE))
                .isEqualTo("paimon-catalog");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_CLUSTER)).isEqualTo("default");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_NAMESPACE))
                .isEqualTo("kling.paimon.catalog");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_DEPLOY_GROUP)).isEqualTo("stable");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_CONF_VERSION)).isEqualTo("");
        assertThat(options.get(RESTCatalogServerOptions.METRICS_CALLER_REGISTRY)).isEqualTo("");
    }

    @Test
    void testBuildMetricsConfigFromOptions() {
        Options options = new Options();
        options.set(RESTCatalogServerOptions.METRICS_SERVICE, "my-service");
        options.set(RESTCatalogServerOptions.METRICS_CLUSTER, "my-cluster");
        options.set(RESTCatalogServerOptions.METRICS_DEPLOY_GROUP, "canary");
        options.set(RESTCatalogServerOptions.METRICS_CONF_VERSION, "v2");

        // No env overrides (all null)
        MetricsConfig config =
                RESTCatalogServer.buildMetricsConfig(
                        options, "test-host", 8080, "3.0.0", null, null, null, null);

        assertThat(config.getService()).isEqualTo("my-service");
        assertThat(config.getCluster()).isEqualTo("my-cluster");
        // Namespace is hardcoded as cluster + ".kling.paimon.catalog"
        assertThat(config.getNamespace()).isEqualTo("my-cluster.kling.paimon.catalog");
        assertThat(config.getDeployGroup()).isEqualTo("canary");
        assertThat(config.getVersion()).isEqualTo("3.0.0");
        assertThat(config.getConfVersion()).isEqualTo("v2");
        assertThat(config.getHost()).isEqualTo("test-host");
        assertThat(config.getPort()).isEqualTo("8080");
    }

    @Test
    void testBuildMetricsConfigDefaults() {
        Options options = new Options();

        MetricsConfig config =
                RESTCatalogServer.buildMetricsConfig(
                        options, "host1", 9090, "dev", null, null, null, null);

        // Aligned with dataset-catalog: namespace is {cluster}.{suffix}
        assertThat(config.getNamespace()).isEqualTo("default.kling.paimon.catalog");
        assertThat(config.getService()).isEqualTo("paimon-catalog");
        assertThat(config.getCluster()).isEqualTo("default");
        assertThat(config.getDeployGroup()).isEqualTo("stable");
        assertThat(config.getVersion()).isEqualTo("dev");
        assertThat(config.getConfVersion()).isEqualTo("");
    }

    @Test
    void testBuildMetricsConfigEnvOverrides() {
        Options options = new Options();
        options.set(RESTCatalogServerOptions.METRICS_CLUSTER, "config-cluster");
        options.set(RESTCatalogServerOptions.METRICS_DEPLOY_GROUP, "config-deploy");

        // Env vars override config options, and cluster dots are normalized to underscores.
        // KCS_IMAGE_VERSION (envDeployGroup) wins over config when GRAY_GROUP_NAME is null.
        MetricsConfig config =
                RESTCatalogServer.buildMetricsConfig(
                        options, "host1", 8080, "dev", "env.cluster", null, "env-deploy", "pod-1");

        assertThat(config.getCluster()).isEqualTo("env_cluster");
        assertThat(config.getNamespace()).isEqualTo("env_cluster.kling.paimon.catalog");
        assertThat(config.getDeployGroup()).isEqualTo("env-deploy");
        // version uses KCS_IMAGE_VERSION when set
        assertThat(config.getVersion()).isEqualTo("env-deploy");
        assertThat(config.getPodName()).isEqualTo("pod-1");
    }

    @Test
    void testBuildMetricsConfigGrayGroupPrecedence() {
        Options options = new Options();
        options.set(RESTCatalogServerOptions.METRICS_DEPLOY_GROUP, "config-deploy");

        // GRAY_GROUP_NAME wins over KCS_IMAGE_VERSION and config for deployGroup.
        // version still follows KCS_IMAGE_VERSION regardless of gray group.
        MetricsConfig config =
                RESTCatalogServer.buildMetricsConfig(
                        options, "host", 8080, "project-ver", null, "gray-123", "image-v5", null);

        assertThat(config.getDeployGroup()).isEqualTo("gray-123");
        assertThat(config.getVersion()).isEqualTo("image-v5");
    }

    @Test
    void testBuildMetricsConfigVersionFallback() {
        Options options = new Options();

        // Case 1: no env -> projectVersion
        MetricsConfig config1 =
                RESTCatalogServer.buildMetricsConfig(
                        options, "host", 8080, "1.2.3", null, null, null, null);
        assertThat(config1.getVersion()).isEqualTo("1.2.3");
        assertThat(config1.getDeployGroup()).isEqualTo("stable");

        // Case 2: KCS_IMAGE_VERSION (envDeployGroup) set -> used for both deployGroup and version
        MetricsConfig config2 =
                RESTCatalogServer.buildMetricsConfig(
                        options, "host", 8080, "1.2.3", null, null, "image-v2", null);
        assertThat(config2.getVersion()).isEqualTo("image-v2");
        assertThat(config2.getDeployGroup()).isEqualTo("image-v2");

        // Case 3: projectVersion "dev" is still used verbatim when no env
        MetricsConfig config3 =
                RESTCatalogServer.buildMetricsConfig(
                        options, "host", 8080, "dev", null, null, null, null);
        assertThat(config3.getVersion()).isEqualTo("dev");
    }
}
