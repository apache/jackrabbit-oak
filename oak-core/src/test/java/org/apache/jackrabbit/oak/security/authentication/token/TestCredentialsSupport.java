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
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;

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

    TestCredentialsSupport(@Nonnull String uid) {
        this.uid = uid;
    }

    @Nonnull
    @Override
    public Set<Class> getCredentialClasses() {
        return ImmutableSet.<Class>of(Creds.class);
    }

    @CheckForNull
    @Override
    public String getUserId(@Nonnull Credentials credentials) {
        if (credentials instanceof Creds) {
            return uid;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Nonnull
    @Override
    public Map<String, ?> getAttributes(@Nonnull Credentials credentials) {
        if (credentials instanceof Creds) {
            return ((Creds) credentials).attributes;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean setAttributes(@Nonnull Credentials credentials, @Nonnull Map<String, ?> attributes) {
        if (credentials instanceof Creds) {
            ((Creds) credentials).attributes.putAll(attributes);
            return true;
        } else {
            throw new IllegalArgumentException();
        }
    }

    static final class Creds implements Credentials {

        private final Map attributes;

        Creds() {
            attributes = new HashMap();
            attributes.put(TokenConstants.TOKEN_ATTRIBUTE, "");
        }
    }
}
