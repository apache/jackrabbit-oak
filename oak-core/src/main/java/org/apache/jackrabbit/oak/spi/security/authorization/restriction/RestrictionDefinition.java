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
package org.apache.jackrabbit.oak.spi.security.authorization.restriction;

import javax.annotation.Nonnull;

/**
 * RestrictionDefinition... TODO
 */
public interface RestrictionDefinition {

    /**
     * The internal oak name of this restriction definition.
     *
     * @return The oak name.
     */
    @Nonnull
    String getName();

    /**
     * The jcr name of this restriction definition.
     *
     * @return The jcr name.
     */
    @Nonnull
    String getJcrName();

    /**
     * The required type as defined by this definition.
     *
     * @return The required type which must be a valid {@link javax.jcr.PropertyType}.
     */
    int getRequiredType();

    /**
     * Indicates if this restriction is mandatory.
     *
     * @return {@code true} if this restriction is mandatory; {@code false} otherwise.
     */
    boolean isMandatory();
}
