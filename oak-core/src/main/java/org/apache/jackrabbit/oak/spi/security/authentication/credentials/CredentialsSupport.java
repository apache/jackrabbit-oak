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

import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

/**
 * Simple helper interface that allows to easily plug support for additional or
 * custom {@link Credentials} implementations during authentication.
 *
 * @see org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule#getSupportedCredentials()
 * @see SimpleCredentialsSupport
 */
public interface CredentialsSupport {

    /**
     * Returns all {@link Credentials credentials} classes supported by this
     * implementation.
     *
     * @return the supported {@link Credentials credentials} classes.
     */
    @Nonnull
    Set<Class> getCredentialClasses();

    /**
     * Retrieves the user identifier from the specified {@code Credentials}.
     * If the specified credentials are not supported or don't contain any
     * user id information this method will return {@code null}.
     *
     * @param credentials The credentials as passed to the repository login.
     * @return The user id present in the given {@code Credentials} or {@code null}.
     */
    @CheckForNull
    String getUserId(@Nonnull Credentials credentials);

    /**
     * Obtains the attributes as present with the specified {@code Credentials}.
     * If the specified credentials are not supported or don't contain any
     * attributes this method will return an empty {@code Map}.
     *
     * @param credentials The credentials as passed to the repository login.
     * @return The credential attributes or an empty {@code Map}.
     */
    @Nonnull
    Map<String, ?> getAttributes(@Nonnull Credentials credentials);

    /**
     * Writes the attributes to the specified {@code Credentials}.
     * If the specified credentials are not supported or doesn't allow to write
     * attributes this method will return {@code false}.
     *
     * @param credentials The credentials as passed to the repository login.
     * @param attributes The attributes to be written to the given credentials.
     * @return {@code true}, if the attributes were set; {@code false} otherwise.
     */
    boolean setAttributes(@Nonnull Credentials credentials, @Nonnull Map<String, ?> attributes);
}