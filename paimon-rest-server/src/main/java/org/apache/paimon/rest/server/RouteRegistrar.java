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

import javax.annotation.Nullable;

/** Interface for handlers that register their own routes with a {@link Router}. */
public interface RouteRegistrar {

    /**
     * Register routes with the given router.
     *
     * @param router the router to register routes with
     * @param prefix the optional URL prefix (e.g. "my-catalog"), may be null
     */
    void registerRoutes(Router router, @Nullable String prefix);
}
