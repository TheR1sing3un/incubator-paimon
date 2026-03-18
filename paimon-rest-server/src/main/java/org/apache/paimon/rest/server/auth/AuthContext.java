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

package org.apache.paimon.rest.server.auth;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Authentication context carrying the identity of an authenticated user.
 *
 * <p>The {@link #properties()} map is an extension point for auth-method-specific metadata such as
 * JWT claims, OAuth2 scopes, or custom attributes.
 */
public class AuthContext {

    public static final AuthContext ANONYMOUS =
            new AuthContext("anonymous", Collections.<String, String>emptyMap());

    private final String userId;
    private final Map<String, String> properties;

    public AuthContext(String userId, Map<String, String> properties) {
        this.userId = userId;
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    public String userId() {
        return userId;
    }

    public Map<String, String> properties() {
        return properties;
    }
}
