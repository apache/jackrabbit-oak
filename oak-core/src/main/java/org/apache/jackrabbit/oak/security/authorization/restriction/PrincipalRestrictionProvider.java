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

import java.util.HashSet;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Restriction provider implementation used for editing access control by
 * principal. It wraps the configured base provider and adds a mandatory
 * restriction definition with name {@link #REP_NODE_PATH} and type {@link Type#PATH PATH}
 * which stores the path of the access controlled node to which a given
 * access control entry will be applied.
 */
public class PrincipalRestrictionProvider implements RestrictionProvider, AccessControlConstants {

    private final RestrictionProvider base;

    public PrincipalRestrictionProvider(@NotNull RestrictionProvider base) {
        this.base = base;
    }

    @NotNull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
        Set<RestrictionDefinition> definitions = new HashSet<>(base.getSupportedRestrictions(oakPath));
        definitions.add(new RestrictionDefinitionImpl(REP_NODE_PATH, Type.PATH, true));
        return definitions;
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value value) throws RepositoryException {
        if (REP_NODE_PATH.equals(oakName)) {
            PropertyState property;
            if (value.getString().isEmpty()) {
                property = PropertyStates.createProperty(oakName, "", Type.STRING);
            } else {
                property = PropertyStates.createProperty(oakName, value);
            }
            return new RestrictionImpl(property, true);
        } else {
            return base.createRestriction(oakPath, oakName, value);
        }
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value... values) throws RepositoryException {
        return base.createRestriction(oakPath, oakName, values);
    }

    @NotNull
    @Override
    public Set<Restriction> readRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) {
        Set<Restriction> restrictions = new HashSet<>(base.readRestrictions(oakPath, aceTree));
        PropertyState property;
        if (oakPath == null) {
            property = PropertyStates.createProperty(REP_NODE_PATH, "", Type.STRING);
        } else {
            property = PropertyStates.createProperty(REP_NODE_PATH, oakPath, Type.PATH);
        }
        restrictions.add(new RestrictionImpl(property, true));
        return restrictions;
    }

    @Override
    public void writeRestrictions(@Nullable String oakPath, @NotNull Tree aceTree, @NotNull Set<Restriction> restrictions) throws RepositoryException {
        Set<Restriction> rs = new HashSet<>(restrictions);
        rs.removeIf(r -> REP_NODE_PATH.equals(r.getDefinition().getName()));
        base.writeRestrictions(oakPath, aceTree, rs);
    }

    @Override
    public void validateRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) throws RepositoryException {
        base.validateRestrictions(oakPath, aceTree);
    }

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
        return base.getPattern(oakPath, tree);
    }

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
        return base.getPattern(oakPath, restrictions);
    }
}
