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
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * PrincipalRestrictionProvider... TODO
 */
public class PrincipalRestrictionProvider implements RestrictionProvider, AccessControlConstants {

    private final RestrictionProvider base;
    private final NamePathMapper namePathMapper;

    public PrincipalRestrictionProvider(RestrictionProvider base, NamePathMapper namePathMapper) {
        this.base = base;
        this.namePathMapper = namePathMapper;
    }

    @Nonnull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(String oakPath) {
        Set<RestrictionDefinition> definitions = new HashSet<RestrictionDefinition>(base.getSupportedRestrictions(oakPath));
        definitions.add(new RestrictionDefinitionImpl(REP_NODE_PATH, PropertyType.PATH, true, namePathMapper));
        return definitions;
    }

    @Nonnull
    @Override
    public Restriction createRestriction(String oakPath, @Nonnull String jcrName, @Nonnull Value value) throws RepositoryException {
        return base.createRestriction(oakPath, jcrName, value);
    }

    @Override
    public Set<Restriction> readRestrictions(String oakPath, Tree aceTree) throws AccessControlException {
        Set<Restriction> restrictions = new HashSet<Restriction>(base.readRestrictions(oakPath, aceTree));
        String value = (oakPath == null) ? "" : oakPath;
        PropertyState nodePathProp = PropertyStates.createProperty(REP_NODE_PATH, value, Type.PATH);
        restrictions.add(new RestrictionImpl(nodePathProp, true, namePathMapper));
        return restrictions;
    }

    @Override
    public void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) throws AccessControlException {
        Iterator<Restriction> it = restrictions.iterator();
        while (it.hasNext()) {
            Restriction r = it.next();
            if (REP_NODE_PATH.equals(r.getName())) {
                it.remove();
            }
        }
        base.writeRestrictions(oakPath, aceTree, restrictions);
    }

    @Override
    public void validateRestrictions(String oakPath, @Nonnull Tree aceTree) throws AccessControlException {
        base.validateRestrictions(oakPath, aceTree);
    }
}