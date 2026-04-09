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

package org.apache.paimon.accelerateindex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** Utility to load {@link AccelerateIndexProvider} implementations via SPI. */
public class AccelerateIndexProviderUtils {

    private AccelerateIndexProviderUtils() {}

    private static final Logger LOG = LoggerFactory.getLogger(AccelerateIndexProviderUtils.class);

    private static final Map<String, AccelerateIndexProvider> PROVIDERS = new HashMap<>();

    static {
        ServiceLoader<AccelerateIndexProvider> serviceLoader =
                ServiceLoader.load(AccelerateIndexProvider.class);
        for (AccelerateIndexProvider provider : serviceLoader) {
            if (PROVIDERS.put(provider.identifier(), provider) != null) {
                LOG.warn(
                        "Found multiple AccelerateIndexProvider for type: "
                                + provider.identifier()
                                + ", choosing the latest one");
            }
        }
    }

    /** Load the provider for the given algorithm type. */
    public static AccelerateIndexProvider load(String algorithm) {
        AccelerateIndexProvider provider = PROVIDERS.get(algorithm);
        if (provider == null) {
            throw new RuntimeException(
                    "Cannot find AccelerateIndexProvider for algorithm: "
                            + algorithm
                            + ". Available: "
                            + PROVIDERS.keySet());
        }
        return provider;
    }
}
