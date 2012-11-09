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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.InvalidItemStateException;
import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PrivilegeManager} implementation reading from and storing privileges
 * into the repository.
 */
public class PrivilegeManagerImpl extends ReadOnlyPrivilegeManager {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PrivilegeManagerImpl.class);

    private final ContentSession contentSession;

    public PrivilegeManagerImpl(Root root, NamePathMapper namePathMapper, ContentSession contentSession) {
        super(root, namePathMapper);
        this.contentSession = contentSession;
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
        PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(contentSession.getLatestRoot());
        writer.writeDefinition(definition);

        // refresh the current root to make sure the definition is visible
        root.refresh();
        return getPrivilege(definition);
    }

    //------------------------------------------------------------< private >---
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
}