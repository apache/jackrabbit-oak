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
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.Sets;
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

/**
 * Restriction provider implementation used for editing access control by
 * principal. It wraps the configured base provider and adds a mandatory
 * restriction definition with name {@link #REP_NODE_PATH} and type {@link Type#PATH PATH}
 * which stores the path of the access controlled node to which a given
 * access control entry will be applied.
 */
public class PrincipalRestrictionProvider implements RestrictionProvider, AccessControlConstants {

    private final RestrictionProvider base;

    public PrincipalRestrictionProvider(RestrictionProvider base) {
        this.base = base;
    }

    @Nonnull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
        Set<RestrictionDefinition> definitions = new HashSet<RestrictionDefinition>(base.getSupportedRestrictions(oakPath));
        definitions.add(new RestrictionDefinitionImpl(REP_NODE_PATH, Type.PATH, true));
        return definitions;
    }

    @Nonnull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value value) throws RepositoryException {
        if (REP_NODE_PATH.equals(oakName) && PropertyType.PATH == value.getType()) {
            return new RestrictionImpl(PropertyStates.createProperty(oakName, value), true);
        } else {
            return base.createRestriction(oakPath, oakName, value);
        }
    }

    @Nonnull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value... values) throws RepositoryException {
        return base.createRestriction(oakPath, oakName, values);
    }

    @Nonnull
    @Override
    public Set<Restriction> readRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree) {
        Set<Restriction> restrictions = new HashSet<Restriction>(base.readRestrictions(oakPath, aceTree));
        String value = (oakPath == null) ? "" : oakPath;
        PropertyState nodePathProp = PropertyStates.createProperty(REP_NODE_PATH, value, Type.PATH);
        restrictions.add(new RestrictionImpl(nodePathProp, true));
        return restrictions;
    }

    @Override
    public void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) throws RepositoryException {
        Iterator<Restriction> it = Sets.newHashSet(restrictions).iterator();
        while (it.hasNext()) {
            Restriction r = it.next();
            if (REP_NODE_PATH.equals(r.getDefinition().getName())) {
                it.remove();
            }
        }
        base.writeRestrictions(oakPath, aceTree, restrictions);
    }

    @Override
    public void validateRestrictions(String oakPath, @Nonnull Tree aceTree) throws RepositoryException {
        base.validateRestrictions(oakPath, aceTree);
    }

    @Nonnull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
        return base.getPattern(oakPath, tree);
    }

    @Nonnull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
        return base.getPattern(oakPath, restrictions);
    }
}