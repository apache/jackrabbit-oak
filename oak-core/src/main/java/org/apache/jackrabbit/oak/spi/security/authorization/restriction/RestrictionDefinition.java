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

import org.apache.jackrabbit.oak.api.Type;

/**
 * The {@code RestrictionDefinition} interface provides methods for
 * discovering the static definition of any additional policy-internal refinements
 * of the access control definitions. These restrictions are intended to be
 * used wherever effects are too fine-grained to be exposed through privilege
 * discovery or define a different restriction mechanism. A common case may be
 * to provide finer-grained access restrictions to individual properties or
 * child nodes of the node to which the policy applies e.g. by means of
 * naming patterns or node type restrictions.
 *
 * Its subclass {@code Restriction} adds methods that are relevant only when
 * a given restriction is "live" after being created and applied to a given
 * policy.
 *
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlList#getRestrictionNames()
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlList#getRestrictionType(String)
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
     * The required type as defined by this definition.
     *
     * @return The required type which must be a valid {@link javax.jcr.PropertyType}.
     */
    @Nonnull
    Type<?> getRequiredType();

    /**
     * Indicates if this restriction is mandatory.
     *
     * @return {@code true} if this restriction is mandatory; {@code false} otherwise.
     */
    boolean isMandatory();
}
