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

package org.apache.paimon.rest.server.handlers;

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Handler for the config endpoint. */
public class ConfigHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigHandler.class);

    @Nullable private final String prefix;
    private final String warehouse;

    public ConfigHandler(@Nullable String prefix, String warehouse) {
        this.prefix = prefix;
        this.warehouse = warehouse;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        router.get(
                "/v1/config",
                (auth, vars, params, body) -> {
                    RESTResponse response = getConfig(params);
                    return new RouteResult(200, response);
                });
    }

    public RESTResponse getConfig(Map<String, String> params) {
        LOG.info("Getting config");
        Map<String, String> defaults = new HashMap<>();
        defaults.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        if (prefix != null) {
            defaults.put(RESTCatalogInternalOptions.PREFIX.key(), prefix);
        }
        return new ConfigResponse(defaults, Collections.emptyMap());
    }
}
