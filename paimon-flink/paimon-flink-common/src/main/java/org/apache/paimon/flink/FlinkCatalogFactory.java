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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Set;

import static org.apache.paimon.flink.FlinkCatalogOptions.DEFAULT_DATABASE;

/** Factory for {@link FlinkCatalog}. */
public class FlinkCatalogFactory implements org.apache.flink.table.factories.CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCatalogFactory.class);

    public static final String IDENTIFIER = "paimon";

    // Flink configuration options for application ID detection.
    // Defined here to avoid hard dependency on flink-yarn/flink-kubernetes modules.
    private static final ConfigOption<String> YARN_APP_ID =
            ConfigOptions.key("yarn.application.id").stringType().noDefaultValue();
    private static final ConfigOption<String> K8S_CLUSTER_ID =
            ConfigOptions.key("kubernetes.cluster-id").stringType().noDefaultValue();
    private static final ConfigOption<String> HA_CLUSTER_ID =
            ConfigOptions.key("high-availability.cluster-id").stringType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public FlinkCatalog createCatalog(Context context) {
        Options catalogOptions = Options.fromMap(context.getOptions());
        // Auto-inject Flink application ID if not already present
        if (!catalogOptions.containsKey(RESTCatalogOptions.APP_ID_KEY)) {
            String appId = resolveFlinkAppId(context);
            if (appId != null && !appId.isEmpty()) {
                catalogOptions.set(RESTCatalogOptions.APP_ID_KEY, appId);
            }
        }
        return createCatalog(
                context.getName(),
                CatalogContext.create(catalogOptions, new FlinkFileIOLoader()),
                context.getClassLoader());
    }

    /**
     * Resolve Flink application ID from multiple sources with fallback: 1. Flink Configuration
     * (yarn.application.id, kubernetes.cluster-id, ha.cluster-id) 2. Environment variables
     * (FLINK_APP_ID)
     */
    @Nullable
    static String resolveFlinkAppId(Context context) {
        try {
            ReadableConfig flinkConf = context.getConfiguration();
            // Try YARN application ID -> K8s cluster ID -> HA cluster ID
            ConfigOption<String>[] candidates =
                    new ConfigOption[] {YARN_APP_ID, K8S_CLUSTER_ID, HA_CLUSTER_ID};
            for (ConfigOption<String> candidate : candidates) {
                String appId = flinkConf.getOptional(candidate).orElse(null);
                if (appId != null && !appId.isEmpty()) {
                    return appId;
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to resolve Flink app ID from configuration", e);
        }
        // Fallback to environment variable
        return System.getenv("FLINK_APP_ID");
    }

    public static FlinkCatalog createCatalog(
            String catalogName, CatalogContext context, ClassLoader classLoader) {
        return new FlinkCatalog(
                CatalogFactory.createCatalog(context, classLoader),
                catalogName,
                context.options().get(DEFAULT_DATABASE),
                context.options());
    }

    public static FlinkCatalog createCatalog(String catalogName, Catalog catalog, Options options) {
        return new FlinkCatalog(catalog, catalogName, Catalog.DEFAULT_DATABASE, options);
    }

    public static Catalog createPaimonCatalog(Options catalogOptions) {
        return CatalogFactory.createCatalog(
                CatalogContext.create(catalogOptions, new FlinkFileIOLoader()));
    }
}
