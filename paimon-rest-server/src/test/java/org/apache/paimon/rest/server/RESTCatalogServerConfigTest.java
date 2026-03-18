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
}
