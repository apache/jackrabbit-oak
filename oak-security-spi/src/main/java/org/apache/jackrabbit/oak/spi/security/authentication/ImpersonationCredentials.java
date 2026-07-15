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
package org.apache.jackrabbit.oak.spi.security.authentication;

import org.apache.jackrabbit.oak.api.AuthInfo;

import javax.jcr.Credentials;

/**
 * Implementation of the JCR {@code Credentials} interface used to distinguish
 * a regular login request from {@link javax.jcr.Session#impersonate(javax.jcr.Credentials)}.
 */
public class ImpersonationCredentials implements Credentials {

    private final Credentials baseCredentials;
    private final AuthInfo authInfo;

    public ImpersonationCredentials(Credentials baseCredentials, AuthInfo authInfo) {
        this.baseCredentials = baseCredentials;
        this.authInfo = authInfo;
    }

    /**
     * Returns the {@code Credentials} originally passed to
     * {@link javax.jcr.Session#impersonate(javax.jcr.Credentials)}.
     *
     * @return the {@code Credentials} originally passed to
     * {@link javax.jcr.Session#impersonate(javax.jcr.Credentials)}.
     */
    public Credentials getBaseCredentials() {
        return baseCredentials;
    }

    /**
     * Returns the {@code AuthInfo} present with the editing session that want
     * to impersonate.
     *
     * @return {@code AuthInfo} present with the editing session that want
     * to impersonate.
     * @see org.apache.jackrabbit.oak.api.ContentSession#getAuthInfo()
     */
    public AuthInfo getImpersonatorInfo() {
        return authInfo;
    }
}