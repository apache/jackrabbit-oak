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
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

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
    @Nonnull
    public ImmutableSet<Class> getCredentialClasses() {
        return ImmutableSet.<Class>of(SimpleCredentials.class);
    }

    @Override
    @CheckForNull
    public String getUserId(@Nonnull Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            return ((SimpleCredentials) credentials).getUserID();
        } else {
            return null;
        }
    }

    @Override
    @Nonnull
    public Map<String, ?> getAttributes(@Nonnull Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            final SimpleCredentials sc = (SimpleCredentials) credentials;
            return Maps.asMap(ImmutableSet.copyOf(sc.getAttributeNames()), new Function<String, Object>() {
                @Nullable
                @Override
                public Object apply(String input) {
                    return sc.getAttribute(input);
                }
            });
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public boolean setAttributes(@Nonnull Credentials credentials, @Nonnull Map<String, ?> attributes) {
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