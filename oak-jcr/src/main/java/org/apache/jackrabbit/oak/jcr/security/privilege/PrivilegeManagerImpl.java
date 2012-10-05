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
package org.apache.jackrabbit.oak.jcr.security.privilege;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeRegistry;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PrivilegeManagerImpl... TODO
 */
public class PrivilegeManagerImpl implements PrivilegeManager {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PrivilegeManagerImpl.class);

    private final PrivilegeProvider provider;
    private final SessionDelegate sessionDelegate;

    public PrivilegeManagerImpl(SessionDelegate sessionDelegate) {
        this.provider = new PrivilegeRegistry(sessionDelegate.getContentSession());
        this.sessionDelegate = sessionDelegate;
    }

    public void refresh() {
        provider.refresh();
    }

    @Override
    public Privilege[] getRegisteredPrivileges() throws RepositoryException {
        Set<Privilege> privileges = new HashSet<Privilege>();
        for (PrivilegeDefinition def : provider.getPrivilegeDefinitions()) {
            privileges.add(new PrivilegeImpl(def));
        }
        return privileges.toArray(new Privilege[privileges.size()]);
    }

    @Override
    public Privilege getPrivilege(String privilegeName) throws RepositoryException {
        PrivilegeDefinition def = provider.getPrivilegeDefinition(getOakName(privilegeName));
        if (def == null) {
            throw new AccessControlException("No such privilege " + privilegeName);
        } else {
            return new PrivilegeImpl(def);
        }
    }

    @Override
    public Privilege registerPrivilege(String privilegeName, boolean isAbstract,
                                       String[] declaredAggregateNames) throws RepositoryException {
        if (privilegeName == null || privilegeName.isEmpty()) {
            throw new RepositoryException("Invalid privilege name " + privilegeName);
        }
        String oakName = getOakName(privilegeName);
        if (oakName == null) {
            throw new NamespaceException("Invalid privilege name " + privilegeName);
        }

        PrivilegeDefinition def = provider.registerDefinition(oakName, isAbstract, getOakNames(declaredAggregateNames));
        sessionDelegate.refresh(true);
        return new PrivilegeImpl(def);
    }

    //------------------------------------------------------------< private >---

    private String getOakName(String jcrName) {
        return sessionDelegate.getNamePathMapper().getOakName(jcrName);
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

    //--------------------------------------------------------------------------
    /**
     * Privilege implementation based on a {@link PrivilegeDefinition}.
     */
    private class PrivilegeImpl implements Privilege {

        private final PrivilegeDefinition definition;

        private PrivilegeImpl(PrivilegeDefinition definition) {
            this.definition = definition;
        }

        //------------------------------------------------------< Privilege >---
        @Override
        public String getName() {
            return getOakName(definition.getName());
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
            for (String pName : declaredAggregateNames) {
                try {
                    declaredAggregates.add(getPrivilege(pName));
                } catch (RepositoryException e) {
                    log.warn("Error while retrieving privilege "+ pName +" contained in " + getName(), e.getMessage());
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