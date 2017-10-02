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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;

public abstract class AbstractRestrictionProvider implements RestrictionProvider, AccessControlConstants {

    private Map<String, RestrictionDefinition> supported;

    public AbstractRestrictionProvider(@Nonnull Map<String, ? extends RestrictionDefinition> definitions) {
        this.supported = ImmutableMap.copyOf(definitions);
    }

    //------------------------------------------------< RestrictionProvider >---
    @Nonnull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(String oakPath) {
        if (isUnsupportedPath(oakPath)) {
            return Collections.emptySet();
        } else {
            return ImmutableSet.copyOf(supported.values());
        }
    }

    @Nonnull
    @Override
    public Restriction createRestriction(String oakPath, @Nonnull String oakName, @Nonnull Value value) throws RepositoryException {
        RestrictionDefinition definition = getDefinition(oakPath, oakName);
        Type<?> requiredType = definition.getRequiredType();
        int tag = requiredType.tag();
        if (tag != PropertyType.UNDEFINED && tag != value.getType()) {
            throw new AccessControlException("Unsupported restriction: Expected value of type " + requiredType);
        }
        PropertyState propertyState;
        if (requiredType.isArray()) {
            propertyState = PropertyStates.createProperty(oakName, ImmutableList.of(value), tag);
        } else {
            propertyState = PropertyStates.createProperty(oakName, value);
        }
        return createRestriction(propertyState, definition);
    }

    @Nonnull
    @Override
    public Restriction createRestriction(String oakPath, @Nonnull String oakName, @Nonnull Value... values) throws RepositoryException {
        RestrictionDefinition definition = getDefinition(oakPath, oakName);
        Type<?> requiredType = definition.getRequiredType();
        for (Value v : values) {
            if (requiredType.tag() != PropertyType.UNDEFINED && requiredType.tag() != v.getType()) {
                throw new AccessControlException("Unsupported restriction: Expected value of type " + requiredType);
            }
        }

        PropertyState propertyState;
        if (requiredType.isArray()) {
            propertyState = PropertyStates.createProperty(oakName, Arrays.asList(values), requiredType.tag());
        } else {
            if (values.length != 1) {
                throw new AccessControlException("Unsupported restriction: Expected single value.");
            }
            propertyState = PropertyStates.createProperty(oakName, values[0]);
        }
        return createRestriction(propertyState, definition);
    }

    @Nonnull
    @Override
    public Set<Restriction> readRestrictions(String oakPath, @Nonnull Tree aceTree) {
        if (isUnsupportedPath(oakPath)) {
            return Collections.emptySet();
        } else {
            Set<Restriction> restrictions = new HashSet<Restriction>();
            for (PropertyState propertyState : getRestrictionsTree(aceTree).getProperties()) {
                String propName = propertyState.getName();
                if (isRestrictionProperty(propName) && supported.containsKey(propName)) {
                    RestrictionDefinition def = supported.get(propName);
                    if (def.getRequiredType() == propertyState.getType()) {
                        restrictions.add(createRestriction(propertyState, def));
                    }
                }
            }
            return restrictions;
        }
    }

    @Override
    public void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) throws RepositoryException {
        // validation of the restrictions is delegated to the commit hook
        // see #validateRestrictions below
        if (!restrictions.isEmpty()) {
            Tree rTree = TreeUtil.getOrAddChild(aceTree, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
            for (Restriction restriction : restrictions) {
                rTree.setProperty(restriction.getProperty());
            }
        }
    }

    @Override
    public void validateRestrictions(String oakPath, @Nonnull Tree aceTree) throws AccessControlException {
        Map<String, PropertyState> restrictionProperties = getRestrictionProperties(aceTree);
        if (isUnsupportedPath(oakPath)) {
            if (!restrictionProperties.isEmpty()) {
                throw new AccessControlException("Restrictions not supported with 'null' path.");
            }
        } else {
            // supported path -> validate restrictions and test if mandatory
            // restrictions are present.
            for (Map.Entry<String, PropertyState> entry : restrictionProperties.entrySet()) {
                String restrName = entry.getKey();
                RestrictionDefinition def = supported.get(restrName);
                if (def == null) {
                    throw new AccessControlException("Unsupported restriction: " + restrName);
                }
                Type<?> type = entry.getValue().getType();
                if (type != def.getRequiredType()) {
                    throw new AccessControlException("Invalid restriction type '" + type + "'. Expected " + def.getRequiredType());
                }
            }
            for (RestrictionDefinition def : supported.values()) {
                if (def.isMandatory() && !restrictionProperties.containsKey(def.getName())) {
                    throw new AccessControlException("Mandatory restriction " + def.getName() + " is missing.");
                }
            }
        }
    }

    //----------------------------------------------------------< protected >---
    /**
     * Returns {@code true} if the specified path is {@code null}. Subclasses may
     * change the default behavior.
     *
     * @param oakPath The path for which a restriction is being created.
     * @return {@code true} if this implementation can create restrictions for
     * the specified {@code oakPath}; {@code false} otherwise.
     */
    protected boolean isUnsupportedPath(@Nullable String oakPath) {
        return oakPath == null;
    }

    /**
     * Returns the tree that contains the restriction of the specified
     * ACE tree.
     *
     * @param aceTree The ACE tree for which the restrictions are being read.
     * @return The tree storing the restriction information.
     */
    @Nonnull
    protected Tree getRestrictionsTree(@Nonnull Tree aceTree) {
        Tree restrictions = aceTree.getChild(REP_RESTRICTIONS);
        if (!restrictions.exists()) {
            // no rep:restrictions tree -> read from aceTree for backwards compatibility
            restrictions = aceTree;
        }
        return restrictions;
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private RestrictionDefinition getDefinition(@Nullable String oakPath, @Nonnull String oakName) throws AccessControlException {
        if (isUnsupportedPath(oakPath)) {
            throw new AccessControlException("Unsupported restriction at " + oakPath);
        }
        RestrictionDefinition definition = supported.get(oakName);
        if (definition == null) {
            throw new AccessControlException("Unsupported restriction: " + oakName);
        }
        return definition;
    }

    @Nonnull
    private Restriction createRestriction(PropertyState propertyState, RestrictionDefinition definition) {
        return new RestrictionImpl(propertyState, definition);
    }

    @Nonnull
    private Map<String, PropertyState> getRestrictionProperties(Tree aceTree) {
        Tree rTree = getRestrictionsTree(aceTree);
        Map<String, PropertyState> restrictionProperties = new HashMap<String, PropertyState>();
        for (PropertyState property : rTree.getProperties()) {
            String name = property.getName();
            if (isRestrictionProperty(name)) {
                restrictionProperties.put(name, property);
            }
        }
        return restrictionProperties;
    }

    private static boolean isRestrictionProperty(String propertyName) {
        return !AccessControlConstants.ACE_PROPERTY_NAMES.contains(propertyName) &&
                !NamespaceRegistry.PREFIX_JCR.equals(Text.getNamespacePrefix(propertyName));
    }
}
