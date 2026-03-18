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

import org.apache.paimon.rest.RESTUtil;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * A single route definition that matches an HTTP method and a path pattern.
 *
 * <p>Path patterns support named variables in curly braces, e.g. {@code
 * /v1/{prefix}/databases/{database}}. Literal segments must match exactly; variable segments match
 * any single path segment and capture the URL-decoded value.
 */
public class Route {

    private final String method;
    private final String pathPattern;
    private final String[] patternSegments;
    private final RouteHandler handler;

    public Route(String method, String pathPattern, RouteHandler handler) {
        this.method = method;
        this.pathPattern = pathPattern;
        this.patternSegments = splitPath(pathPattern);
        this.handler = handler;
    }

    /**
     * Attempts to match the given HTTP method and path segments against this route.
     *
     * @return a map of path variable name to decoded value if matched, or null if no match
     */
    @Nullable
    public Map<String, String> match(String requestMethod, String[] requestSegments) {
        if (!method.equals(requestMethod)) {
            return null;
        }
        if (patternSegments.length != requestSegments.length) {
            return null;
        }
        Map<String, String> variables = new HashMap<>();
        for (int i = 0; i < patternSegments.length; i++) {
            String pattern = patternSegments[i];
            String actual = requestSegments[i];
            if (pattern.startsWith("{") && pattern.endsWith("}")) {
                String varName = pattern.substring(1, pattern.length() - 1);
                variables.put(varName, RESTUtil.decodeString(actual));
            } else if (!pattern.equals(actual)) {
                return null;
            }
        }
        return variables;
    }

    public RouteHandler handler() {
        return handler;
    }

    public String pathPattern() {
        return pathPattern;
    }

    public String method() {
        return method;
    }

    static String[] splitPath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.isEmpty()) {
            return new String[0];
        }
        return path.split("/");
    }
}
