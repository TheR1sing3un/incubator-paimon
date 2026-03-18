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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A simple HTTP router that matches requests against registered routes.
 *
 * <p>Routes are matched in registration order (first match wins), so more-specific routes should be
 * registered before less-specific ones.
 */
public class Router {

    private final List<Route> routes = new ArrayList<>();

    public void addRoute(String method, String pathPattern, RouteHandler handler) {
        routes.add(new Route(method, pathPattern, handler));
    }

    public void get(String pathPattern, RouteHandler handler) {
        addRoute("GET", pathPattern, handler);
    }

    public void post(String pathPattern, RouteHandler handler) {
        addRoute("POST", pathPattern, handler);
    }

    public void delete(String pathPattern, RouteHandler handler) {
        addRoute("DELETE", pathPattern, handler);
    }

    /**
     * Match a route without executing the handler. This supports two-phase dispatch where route
     * matching is separated from execution, enabling audit logging even when the handler throws.
     *
     * @return the matched route info, or null if no route matches
     */
    @Nullable
    public RouteMatch findMatch(String method, String path) {
        String[] segments = Route.splitPath(path);
        for (Route route : routes) {
            Map<String, String> vars = route.match(method, segments);
            if (vars != null) {
                return new RouteMatch(route.handler(), route.method(), route.pathPattern(), vars);
            }
        }
        return null;
    }

    /**
     * Dispatch an HTTP request to the first matching route.
     *
     * @return the route result, or a 404 result if no route matches
     */
    public RouteResult dispatch(
            AuthContext authContext,
            String method,
            String path,
            Map<String, String> queryParams,
            String body)
            throws Exception {
        return dispatchWithContext(authContext, method, path, queryParams, body).routeResult();
    }

    /**
     * Dispatch an HTTP request and return matched route context for audit logging.
     *
     * @return a DispatchContext containing the result, matched pattern, and path variables
     */
    public DispatchContext dispatchWithContext(
            AuthContext authContext,
            String method,
            String path,
            Map<String, String> queryParams,
            String body)
            throws Exception {
        String[] segments = Route.splitPath(path);
        for (Route route : routes) {
            Map<String, String> vars = route.match(method, segments);
            if (vars != null) {
                RouteResult result = route.handler().handle(authContext, vars, queryParams, body);
                return new DispatchContext(result, route.method(), route.pathPattern(), vars);
            }
        }
        return new DispatchContext(
                new RouteResult(404, null), method, null, Collections.<String, String>emptyMap());
    }

    /** Context returned by {@link #dispatchWithContext}, includes matched route metadata. */
    public static class DispatchContext {
        private final RouteResult routeResult;
        private final String method;
        private final String matchedPattern;
        private final Map<String, String> pathVariables;

        public DispatchContext(
                RouteResult routeResult,
                String method,
                String matchedPattern,
                Map<String, String> pathVariables) {
            this.routeResult = routeResult;
            this.method = method;
            this.matchedPattern = matchedPattern;
            this.pathVariables = pathVariables;
        }

        public RouteResult routeResult() {
            return routeResult;
        }

        public String method() {
            return method;
        }

        public String matchedPattern() {
            return matchedPattern;
        }

        public Map<String, String> pathVariables() {
            return pathVariables;
        }
    }

    /** Result of route matching without handler execution. */
    public static class RouteMatch {
        private final RouteHandler handler;
        private final String method;
        private final String matchedPattern;
        private final Map<String, String> pathVariables;

        RouteMatch(
                RouteHandler handler,
                String method,
                String matchedPattern,
                Map<String, String> pathVariables) {
            this.handler = handler;
            this.method = method;
            this.matchedPattern = matchedPattern;
            this.pathVariables = pathVariables;
        }

        public RouteHandler handler() {
            return handler;
        }

        public String method() {
            return method;
        }

        public String matchedPattern() {
            return matchedPattern;
        }

        public Map<String, String> pathVariables() {
            return pathVariables;
        }
    }
}
