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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;

import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy implementation of {@link CredentialsSupport} that only supports
 * {@link org.apache.jackrabbit.oak.security.authentication.token.TestCredentialsSupport.Creds}
 * and always returns the same user ID upon {@link CredentialsSupport#getUserId(Credentials)}.
 */
public class TestCredentialsSupport implements CredentialsSupport {

    private final String uid;

    TestCredentialsSupport() {
        this.uid = null;
    }

    TestCredentialsSupport(@NotNull String uid) {
        this.uid = uid;
    }

    @NotNull
    @Override
    public Set<Class> getCredentialClasses() {
        return Set.of(Creds.class);
    }

    @Nullable
    @Override
    public String getUserId(@NotNull Credentials credentials) {
        if (credentials instanceof Creds) {
            return uid;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @NotNull
    @Override
    public Map<String, ?> getAttributes(@NotNull Credentials credentials) {
        if (credentials instanceof Creds) {
            return ((Creds) credentials).attributes;
        } else {
            return Map.of();
        }
    }

    @Override
    public boolean setAttributes(@NotNull Credentials credentials, @NotNull Map<String, ?> attributes) {
        if (credentials instanceof Creds) {
            ((Creds) credentials).attributes.putAll(attributes);
            return true;
        } else {
            return false;
        }
    }

    static final class Creds implements Credentials {

        private final Map<String, Object> attributes;

        Creds() {
            attributes = new HashMap<>();
            attributes.put(TokenConstants.TOKEN_ATTRIBUTE, TokenConstants.TOKEN_ATTRIBUTE_DO_CREATE);
        }
    }
}
