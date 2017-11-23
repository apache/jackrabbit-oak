/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.security.authentication.callback;

import javax.annotation.CheckForNull;
import javax.jcr.Credentials;
import javax.security.auth.callback.Callback;

/**
 * Callback implementation to retrieve {@code Credentials}.
 */
public class CredentialsCallback implements Callback {

    private Credentials credentials;

    /**
     * Returns the {@link Credentials} that have been set before using
     * {@link #setCredentials(javax.jcr.Credentials)}.
     *
     * @return The {@link Credentials} to be used for authentication or {@code null}.
     */
    @CheckForNull
    public Credentials getCredentials() {
        return credentials;
    }

    /**
     * Set the credentials.
     *
     * @param credentials The credentials to be used in the authentication
     * process. They may be null if no credentials have been specified in
     * {@link org.apache.jackrabbit.oak.api.ContentRepository#login(javax.jcr.Credentials, String)}
     */
    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }
}