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
import javax.jcr.PropertyType;

import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.api.Type;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of the {@link RestrictionDefinition} interface.
 */
public class RestrictionDefinitionImpl implements RestrictionDefinition {

    private final String name;
    private final Type<?> type;
    private final boolean isMandatory;

    /**
     * Create a new instance.
     *
     * @param name           The oak name of the restriction definition.
     * @param type           The required type of this definition. Any valid JCR
     *                       {@link javax.jcr.PropertyType} except {@link javax.jcr.PropertyType#UNDEFINED}
     *                       is allowed.
     * @param isMandatory    A boolean indicating if the restriction is mandatory.
     */
    public RestrictionDefinitionImpl(@Nonnull String name, Type<?> type, boolean isMandatory) {
        this.name = checkNotNull(name);
        if (type.tag() == PropertyType.UNDEFINED) {
            throw new IllegalArgumentException("'undefined' is not a valid required definition type.");
        }
        this.type = type;
        this.isMandatory = isMandatory;
    }

    //----------------------------------------------< RestrictionDefinition >---
    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public Type<?> getRequiredType() {
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
