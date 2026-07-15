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

import javax.jcr.Credentials;

/**
 * {@code PreAuthenticatedLogin} is used as marker in the shared map of the login context. it indicates that the
 * respective user is pre authenticated on an external system. Note that is class is only used internally by the
 * login modules and cannot be "abused" from outside.
 */
public final class PreAuthenticatedLogin {

    public static final Credentials PRE_AUTHENTICATED = new Credentials() { };

    private final String userId;

    public PreAuthenticatedLogin(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }
}