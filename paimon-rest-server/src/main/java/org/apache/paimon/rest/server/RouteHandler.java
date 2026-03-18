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

import org.apache.paimon.rest.server.auth.AuthContext;

import java.util.Map;

/** Functional interface for handling a matched route. */
@FunctionalInterface
public interface RouteHandler {

    /**
     * Handle an HTTP request that matched a route.
     *
     * @param authContext the authenticated user context
     * @param pathVariables variables extracted from the URL path pattern
     * @param queryParams flattened query string parameters
     * @param body the request body as a string
     * @return the route result containing status code and optional response
     */
    RouteResult handle(
            AuthContext authContext,
            Map<String, String> pathVariables,
            Map<String, String> queryParams,
            String body)
            throws Exception;
}
