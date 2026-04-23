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

package org.apache.paimon.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkCatalogFactory}, focusing on the app-id resolution logic. */
class FlinkCatalogFactoryTest {

    @Test
    void testResolveFlinkAppIdFromYarn() {
        Configuration flinkConf = new Configuration();
        flinkConf.setString("yarn.application.id", "application_1714000000_0042");
        CatalogFactory.Context context = createContext(flinkConf);

        assertThat(FlinkCatalogFactory.resolveFlinkAppId(context))
                .isEqualTo("application_1714000000_0042");
    }

    @Test
    void testResolveFlinkAppIdFromK8s() {
        Configuration flinkConf = new Configuration();
        flinkConf.setString("kubernetes.cluster-id", "spark-abc123def456");
        CatalogFactory.Context context = createContext(flinkConf);

        assertThat(FlinkCatalogFactory.resolveFlinkAppId(context)).isEqualTo("spark-abc123def456");
    }

    @Test
    void testResolveFlinkAppIdFromHA() {
        Configuration flinkConf = new Configuration();
        flinkConf.setString("high-availability.cluster-id", "ha-cluster-001");
        CatalogFactory.Context context = createContext(flinkConf);

        assertThat(FlinkCatalogFactory.resolveFlinkAppId(context)).isEqualTo("ha-cluster-001");
    }

    @Test
    void testResolveFlinkAppIdPriority() {
        // YARN takes priority over K8s and HA
        Configuration flinkConf = new Configuration();
        flinkConf.setString("yarn.application.id", "yarn-app-001");
        flinkConf.setString("kubernetes.cluster-id", "k8s-cluster-001");
        flinkConf.setString("high-availability.cluster-id", "ha-cluster-001");
        CatalogFactory.Context context = createContext(flinkConf);

        assertThat(FlinkCatalogFactory.resolveFlinkAppId(context)).isEqualTo("yarn-app-001");
    }

    @Test
    void testResolveFlinkAppIdK8sOverHA() {
        // K8s takes priority over HA
        Configuration flinkConf = new Configuration();
        flinkConf.setString("kubernetes.cluster-id", "k8s-cluster-001");
        flinkConf.setString("high-availability.cluster-id", "ha-cluster-001");
        CatalogFactory.Context context = createContext(flinkConf);

        assertThat(FlinkCatalogFactory.resolveFlinkAppId(context)).isEqualTo("k8s-cluster-001");
    }

    @Test
    void testResolveFlinkAppIdSkipsEmptyValues() {
        Configuration flinkConf = new Configuration();
        flinkConf.setString("yarn.application.id", "");
        flinkConf.setString("kubernetes.cluster-id", "k8s-cluster-001");
        CatalogFactory.Context context = createContext(flinkConf);

        assertThat(FlinkCatalogFactory.resolveFlinkAppId(context)).isEqualTo("k8s-cluster-001");
    }

    @Test
    void testResolveFlinkAppIdNoneConfigured() {
        // When no config is set, falls back to System.getenv("FLINK_APP_ID") which is
        // typically null in test environments
        Configuration flinkConf = new Configuration();
        CatalogFactory.Context context = createContext(flinkConf);

        String result = FlinkCatalogFactory.resolveFlinkAppId(context);
        // result is System.getenv("FLINK_APP_ID"), typically null in CI
        assertThat(result).isEqualTo(System.getenv("FLINK_APP_ID"));
    }

    private static CatalogFactory.Context createContext(Configuration flinkConf) {
        return new FactoryUtil.DefaultCatalogContext(
                "test-catalog",
                Collections.emptyMap(),
                flinkConf,
                FlinkCatalogFactoryTest.class.getClassLoader());
    }
}
