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
package org.apache.jackrabbit.oak.spi.security.authentication.credentials;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.guava.common.collect.Maps;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport}
 * interface that handles {@link javax.jcr.SimpleCredentials}.
 */
public final class SimpleCredentialsSupport implements CredentialsSupport {

    private static final SimpleCredentialsSupport INSTANCE = new SimpleCredentialsSupport();

    private SimpleCredentialsSupport() {};

    public static CredentialsSupport getInstance() {
        return INSTANCE;
    }

    @Override
    @NotNull
    public Set<Class> getCredentialClasses() {
        return Set.of(SimpleCredentials.class);
    }

    @Override
    @Nullable
    public String getUserId(@NotNull Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            return ((SimpleCredentials) credentials).getUserID();
        } else {
            return null;
        }
    }

    @Override
    @NotNull
    public Map<String, ?> getAttributes(@NotNull Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            final SimpleCredentials sc = (SimpleCredentials) credentials;
            return Maps.asMap(Set.of(sc.getAttributeNames()), sc::getAttribute);
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public boolean setAttributes(@NotNull Credentials credentials, @NotNull Map<String, ?> attributes) {
        if (credentials instanceof SimpleCredentials) {
            SimpleCredentials sc = (SimpleCredentials) credentials;
            for (Map.Entry<String, ?> entry : attributes.entrySet()) {
                sc.setAttribute(entry.getKey(), entry.getValue());
            }
            return true;
        } else {
            return false;
        }
    }
}
