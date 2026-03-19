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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Route} and {@link Router}. */
class RouteTest {

    private static final RouteHandler DUMMY_HANDLER =
            (authContext, pathVariables, queryParams, body) -> new RouteResult(200, null);

    // ---- Route tests ----

    @Test
    void testExactPathMatch() {
        Route route = new Route("GET", "/v1/config", DUMMY_HANDLER);
        String[] segments = Route.splitPath("/v1/config");

        Map<String, String> vars = route.match("GET", segments);

        assertThat(vars).isNotNull().isEmpty();
    }

    @Test
    void testPathVariableExtraction() {
        Route route = new Route("GET", "/v1/{prefix}/databases/{database}", DUMMY_HANDLER);
        String[] segments = Route.splitPath("/v1/my_catalog/databases/prod_db");

        Map<String, String> vars = route.match("GET", segments);

        assertThat(vars)
                .isNotNull()
                .containsEntry("prefix", "my_catalog")
                .containsEntry("database", "prod_db")
                .hasSize(2);
    }

    @Test
    void testMethodMismatch() {
        Route route = new Route("GET", "/v1/config", DUMMY_HANDLER);
        String[] segments = Route.splitPath("/v1/config");

        Map<String, String> vars = route.match("POST", segments);

        assertThat(vars).isNull();
    }

    @Test
    void testSegmentCountMismatch() {
        Route route = new Route("GET", "/v1/{prefix}/databases/{database}", DUMMY_HANDLER);

        // Too few segments.
        String[] tooFew = Route.splitPath("/v1/my_catalog");
        assertThat(route.match("GET", tooFew)).isNull();

        // Too many segments.
        String[] tooMany = Route.splitPath("/v1/my_catalog/databases/prod_db/extra");
        assertThat(route.match("GET", tooMany)).isNull();
    }

    @Test
    void testUrlDecoding() {
        Route route = new Route("GET", "/v1/{prefix}/databases/{database}", DUMMY_HANDLER);
        String[] segments = Route.splitPath("/v1/cat/databases/my%20db");

        Map<String, String> vars = route.match("GET", segments);

        assertThat(vars).isNotNull().containsEntry("database", "my db");
    }

    // ---- Router tests ----

    @Test
    void testRouterFirstMatchWins() throws Exception {
        Router router = new Router();

        RouteHandler firstHandler =
                (authContext, pathVariables, queryParams, body) -> new RouteResult(200, null);
        RouteHandler secondHandler =
                (authContext, pathVariables, queryParams, body) -> new RouteResult(201, null);

        router.get("/v1/config", firstHandler);
        router.get("/v1/config", secondHandler);

        RouteResult result =
                router.dispatch(
                        AuthContext.ANONYMOUS, "GET", "/v1/config", Collections.emptyMap(), "");

        assertThat(result.status()).isEqualTo(200);
    }

    @Test
    void testRouterFindMatch() {
        Router router = new Router();
        router.get("/v1/{prefix}/databases/{database}", DUMMY_HANDLER);

        Router.RouteMatch match = router.findMatch("GET", "/v1/my_catalog/databases/prod_db");

        assertThat(match).isNotNull();
        assertThat(match.handler()).isSameAs(DUMMY_HANDLER);
        assertThat(match.method()).isEqualTo("GET");
        assertThat(match.matchedPattern()).isEqualTo("/v1/{prefix}/databases/{database}");
        assertThat(match.pathVariables())
                .containsEntry("prefix", "my_catalog")
                .containsEntry("database", "prod_db");
    }

    @Test
    void testRouter404WhenNoMatch() throws Exception {
        Router router = new Router();
        router.get("/v1/config", DUMMY_HANDLER);

        RouteResult result =
                router.dispatch(
                        AuthContext.ANONYMOUS,
                        "GET",
                        "/v1/nonexistent",
                        Collections.emptyMap(),
                        "");

        assertThat(result.status()).isEqualTo(404);
        assertThat(result.response()).isNull();
    }
}
