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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * RestrictionDefinitionImpl... TODO
 */
class RestrictionDefinitionImpl implements RestrictionDefinition {

    private final String name;
    private final int type;
    private final boolean isMandatory;
    private final NamePathMapper namePathMapper;

    /**
     * Create a new instance.
     *
     * @param name           The oak name of the restriction definition.
     * @param type           The required type of this definition. Any valid JCR
     *                       {@link javax.jcr.PropertyType} except {@link javax.jcr.PropertyType#UNDEFINED}
     *                       is allowed.
     * @param isMandatory    A boolean indicating if the restriction is mandatory.
     * @param namePathMapper The name path mapper used to calculate the JCR name.
     */
    RestrictionDefinitionImpl(@Nonnull String name, int type, boolean isMandatory,
                              @Nonnull NamePathMapper namePathMapper) {
        this.name = checkNotNull(name);
        if (type == PropertyType.UNDEFINED) {
            throw new IllegalArgumentException("'undefined' is not a valid required definition type.");
        }
        this.type = type;
        this.isMandatory = isMandatory;
        this.namePathMapper = checkNotNull(namePathMapper);
    }

    NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    //----------------------------------------------< RestrictionDefinition >---
    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public String getJcrName() {
        return namePathMapper.getJcrName(getName());
    }

    @Override
    public int getRequiredType() {
        return type;
    }

    @Override
    public boolean isMandatory() {
        return isMandatory;
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public int hashCode() {
        return Objects.hashCode(name, type, isMandatory);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o instanceof RestrictionDefinitionImpl) {
            RestrictionDefinitionImpl other = (RestrictionDefinitionImpl) o;
            return type == other.type && isMandatory == other.isMandatory && name.equals(other.name);
        }

        return false;
    }
}
