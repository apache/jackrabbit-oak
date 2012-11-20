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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PrivilegeManager} implementation reading from and storing privileges
 * into the repository.
 *
 * TODO: review if jcr:all should be present in the content as well (updated in the privilege commit validator)
 */
public class PrivilegeManagerImpl implements PrivilegeManager {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PrivilegeManagerImpl.class);

    final Root root;
    final NamePathMapper namePathMapper;

    public PrivilegeManagerImpl(Root root, NamePathMapper namePathMapper) {
        this.root = root;
        this.namePathMapper = namePathMapper;
    }

    @Override
    public Privilege[] getRegisteredPrivileges() throws RepositoryException {
        Set<Privilege> privileges = new HashSet<Privilege>();
        for (PrivilegeDefinition def : getPrivilegeDefinitions()) {
            privileges.add(getPrivilege(def));
        }
        return privileges.toArray(new Privilege[privileges.size()]);
    }

    @Override
    public Privilege getPrivilege(String privilegeName) throws RepositoryException {
        PrivilegeDefinition def = getPrivilegeDefinition(getOakName(privilegeName));
        if (def == null) {
            throw new AccessControlException("No such privilege " + privilegeName);
        } else {
            return getPrivilege(def);
        }
    }

    @Override
    public Privilege registerPrivilege(String privilegeName, boolean isAbstract,
                                       String[] declaredAggregateNames) throws RepositoryException {
        if (root.hasPendingChanges()) {
            throw new InvalidItemStateException("Attempt to register a new privilege while there are pending changes.");
        }
        if (privilegeName == null || privilegeName.isEmpty()) {
            throw new RepositoryException("Invalid privilege name " + privilegeName);
        }
        String oakName = getOakName(privilegeName);
        if (oakName == null) {
            throw new NamespaceException("Invalid privilege name " + privilegeName);
        }

        PrivilegeDefinition definition = new PrivilegeDefinitionImpl(oakName, isAbstract, getOakNames(declaredAggregateNames));
        PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(getWriteRoot());
        writer.writeDefinition(definition);

        // refresh the current root to make sure the definition is visible
        root.refresh();
        return getPrivilege(definition);
    }

    //------------------------------------------------------------< private >---
    private Root getWriteRoot() throws UnsupportedRepositoryOperationException {
        if (root instanceof RootImpl) {
            return ((RootImpl) root).getLatest();
        } else {
            throw new UnsupportedRepositoryOperationException("Privilege registration not supported");
        }
    }

    private Set<String> getOakNames(String[] jcrNames) throws RepositoryException {
        Set<String> oakNames;
        if (jcrNames == null || jcrNames.length == 0) {
            oakNames = Collections.emptySet();
        } else {
            oakNames = new HashSet<String>(jcrNames.length);
            for (String jcrName : jcrNames) {
                String oakName = getOakName(jcrName);
                if (oakName == null) {
                    throw new RepositoryException("Invalid name " + jcrName);
                }
                oakNames.add(oakName);
            }
        }
        return oakNames;
    }

    @CheckForNull
    String getOakName(String jcrName) {
        return namePathMapper.getOakName(jcrName);
    }

    @Nonnull
    Privilege getPrivilege(PrivilegeDefinition definition) {
        return new PrivilegeImpl(definition);
    }

    @Nonnull
    private PrivilegeDefinition[] getPrivilegeDefinitions() {
        Map<String, PrivilegeDefinition> definitions = getReader().readDefinitions();
        definitions.put(PrivilegeConstants.JCR_ALL, getJcrAllDefinition(definitions));
        return definitions.values().toArray(new PrivilegeDefinition[definitions.size()]);
    }

    @CheckForNull
    private PrivilegeDefinition getPrivilegeDefinition(String oakName) {
        if (PrivilegeConstants.JCR_ALL.equals(oakName)) {
            return getJcrAllDefinition(getReader().readDefinitions());
        } else {
            return getReader().readDefinition(oakName);
        }
    }

    @Nonnull
    private PrivilegeDefinitionReader getReader() {
        return new PrivilegeDefinitionReader(root);
    }

    @Nonnull
    private static PrivilegeDefinition getJcrAllDefinition(Map<String, PrivilegeDefinition> definitions) {
        return new PrivilegeDefinitionImpl(PrivilegeConstants.JCR_ALL, false, definitions.keySet());
    }

    //--------------------------------------------------------------------------
    /**
     * Privilege implementation based on a {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition}.
     */
    private class PrivilegeImpl implements Privilege {

        private final PrivilegeDefinition definition;

        private PrivilegeImpl(PrivilegeDefinition definition) {
            this.definition = definition;
        }

        //------------------------------------------------------< Privilege >---
        @Override
        public String getName() {
            return namePathMapper.getJcrName(definition.getName());
        }

        @Override
        public boolean isAbstract() {
            return definition.isAbstract();
        }

        @Override
        public boolean isAggregate() {
            return !definition.getDeclaredAggregateNames().isEmpty();
        }

        @Override
        public Privilege[] getDeclaredAggregatePrivileges() {
            Set<String> declaredAggregateNames = definition.getDeclaredAggregateNames();
            Set<Privilege> declaredAggregates = new HashSet<Privilege>(declaredAggregateNames.size());
            for (String oakName : declaredAggregateNames) {
                if (oakName.equals(definition.getName())) {
                    log.warn("Found cyclic privilege aggregation -> ignore declared aggregate " + oakName);
                    continue;
                }
                PrivilegeDefinition def = getPrivilegeDefinition(oakName);
                if (def != null) {
                    declaredAggregates.add(getPrivilege(def));
                } else {
                    log.warn("Invalid privilege '{}' in declared aggregates of '{}'", oakName, getName());
                }
            }
            return declaredAggregates.toArray(new Privilege[declaredAggregates.size()]);
        }

        @Override
        public Privilege[] getAggregatePrivileges() {
            Set<Privilege> aggr = new HashSet<Privilege>();
            for (Privilege decl : getDeclaredAggregatePrivileges()) {
                aggr.add(decl);
                if (decl.isAggregate()) {
                    // TODO: defensive check to prevent circular aggregation that might occur with inconsistent repositories
                    aggr.addAll(Arrays.asList(decl.getAggregatePrivileges()));
                }
            }
            return aggr.toArray(new Privilege[aggr.size()]);
        }

        //---------------------------------------------------------< Object >---
        @Override
        public int hashCode() {
            return definition.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof PrivilegeImpl) {
                return definition.equals(((PrivilegeImpl) o).definition);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return "Privilege " + definition.getName();
        }
    }
}