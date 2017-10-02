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

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * A {@code Restriction} object represents a "live" restriction object that
 * has been created using the Jackrabbit specific extensions of the
 * {@link javax.jcr.security.AccessControlEntry AccessControlEntry} interface.
 *
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlList#addEntry(java.security.Principal, javax.jcr.security.Privilege[], boolean, java.util.Map)
 */
public interface Restriction {

    /**
     * Returns the underlying restriction definition.
     *
     * @return the restriction definition that applies to this restriction.
     */
    @Nonnull
    RestrictionDefinition getDefinition();

    /**
     * The OAK property state associated with this restriction.
     *
     * @return An {@code PropertyState}.
     */
    @Nonnull
    PropertyState getProperty();
}
