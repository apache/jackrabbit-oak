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

import java.security.AccessControlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.util.Text;

/**
 * RestrictionProviderImpl... TODO
 */
public class RestrictionProviderImpl implements RestrictionProvider {

    private Map<String, RestrictionDefinition> supported;
    private NamePathMapper namePathMapper;

    public RestrictionProviderImpl(NamePathMapper namePathMapper) {
        RestrictionDefinition glob = new RestrictionDefinitionImpl(AccessControlConstants.REP_GLOB, PropertyType.STRING, false);
        this.supported = Collections.singletonMap(AccessControlConstants.REP_GLOB, glob);
        this.namePathMapper = namePathMapper;
    }

    @Nonnull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(String path) {
        if (path == null) {
            return Collections.emptySet();
        } else {
            return ImmutableSet.copyOf(supported.values());
        }
    }

    @Override
    public Restriction createRestriction(String path, String jcrName, Value value) throws RepositoryException {
        String oakName = namePathMapper.getOakName(jcrName);
        RestrictionDefinition definition = (path == null) ? null : supported.get(oakName);
        if (definition == null) {
            throw new AccessControlException("Unsupported restriction: " + jcrName);
        }
        int requiredType = definition.getRequiredType();
        if (requiredType != PropertyType.UNDEFINED && requiredType != value.getType()) {
            throw new AccessControlException("Unsupported restriction: Expected value of type " + PropertyType.nameFromValue(definition.getRequiredType()));
        }
        PropertyState propertyState = PropertyStates.createProperty(oakName, value);
        return new RestrictionImpl(propertyState, requiredType, definition.isMandatory());
    }

    @Override
    public Set<Restriction> readRestrictions(String path, Tree aceTre) throws javax.jcr.security.AccessControlException {
        // TODO
        return null;
    }

    @Override
    public void writeRestrictions(String path, Tree aceTree, Set<Restriction> restrictions) throws javax.jcr.security.AccessControlException {
        // TODO

    }

    @Override
    public void validateRestrictions(String path, Tree aceTree) throws javax.jcr.security.AccessControlException {
        Tree restrictions;
        if (aceTree.hasChild(AccessControlConstants.REP_RESTRICTIONS)) {
            restrictions = aceTree.getChild(AccessControlConstants.REP_RESTRICTIONS);
        } else {
            // backwards compatibility
            restrictions = aceTree;
        }

        Map<String,PropertyState> restrictionProperties = new HashMap<String, PropertyState>();
        for (PropertyState property : restrictions.getProperties()) {
            String name = property.getName();
            String prefix = Text.getNamespacePrefix(name);
            if (!NamespaceRegistry.PREFIX_JCR.equals(prefix) && !AccessControlConstants.AC_PROPERTY_NAMES.contains(name)) {
                restrictionProperties.put(name, property);
            }
        }

        if (path == null && !restrictionProperties.isEmpty()) {
            throw new AccessControlException("Restrictions not supported with 'null' path.");
        }
        for (String restrName : restrictionProperties.keySet()) {
            RestrictionDefinition def = supported.get(restrName);
            if (def == null || restrictionProperties.get(restrName).getType().tag() != def.getRequiredType()) {
                throw new AccessControlException("Unsupported restriction: " + restrName);
            }
        }
        for (RestrictionDefinition def : supported.values()) {
            if (def.isMandatory() && !restrictionProperties.containsKey(def.getName())) {
                throw new AccessControlException("Mandatory restriction " + def.getName() + " is missing.");
            }
        }
    }

    private static class RestrictionImpl implements Restriction {

        private final PropertyState property;
        private final int requiredType;
        private final boolean isMandatory;

        private RestrictionImpl(PropertyState property, int requiredType, boolean isMandatory) {
            this.property = property;
            this.requiredType = requiredType;
            this.isMandatory = isMandatory;
        }

        @Nonnull
        @Override
        public PropertyState getProperty() {
            return property;
        }

        @Nonnull
        @Override
        public String getName() {
            return property.getName();
        }

        @Nonnull
        @Override
        public int getRequiredType() {
            return requiredType;
        }

        @Override
        public boolean isMandatory() {
            return isMandatory;
        }
    }
}
