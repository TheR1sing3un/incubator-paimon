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

import javax.annotation.Nullable;

/**
 * Token authenticator interface. This is the single extension point for authentication.
 *
 * <p>The flow is: HTTP request comes in → token is extracted from the header → this authenticator
 * is called with the token → it returns an {@link AuthContext} on success or throws {@link
 * AuthenticationException} on failure.
 *
 * <p>Implementations may call an external auth service, validate a JWT, query a database, etc. The
 * default (when no auth URL is configured) simply returns {@link AuthContext#ANONYMOUS}.
 */
@FunctionalInterface
public interface TokenAuthenticator {

    /**
     * Authenticate a token extracted from the HTTP request.
     *
     * @param token the token string extracted from the request header, may be null if no token was
     *     provided
     * @return the authentication context for this request
     * @throws AuthenticationException if the token is missing, invalid, or rejected
     */
    AuthContext authenticate(@Nullable String token) throws AuthenticationException;
}
