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
import java.util.Set;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * PrivilegeDefinitionWriter is responsible for writing privilege definitions
 * to the repository without applying any validation checks.
 */
class PrivilegeDefinitionWriter implements PrivilegeConstants {

    private final Root root;

    PrivilegeDefinitionWriter(Root root) {
        this.root = root;
    }

    void writeDefinition(PrivilegeDefinition definition) throws RepositoryException {
        writeDefinitions(Collections.singleton(definition));
    }

    void writeDefinitions(Set<PrivilegeDefinition> definitions) throws RepositoryException {
        try {
            // make sure the privileges path is defined
            Tree privilegesTree = root.getTree(PRIVILEGES_PATH);
            if (privilegesTree == null) {
                throw new RepositoryException("Repository doesn't contain node " + PRIVILEGES_PATH);
            }

            NodeUtil privilegesNode = new NodeUtil(privilegesTree);
            for (PrivilegeDefinition definition : definitions) {
                writePrivilegeNode(privilegesNode, definition);
            }

            // delegate validation to the commit validation (see above)
            root.commit();

        } catch (CommitFailedException e) {
            Throwable t = e.getCause();
            if (t instanceof RepositoryException) {
                throw (RepositoryException) t;
            } else {
                throw new RepositoryException(e.getMessage());
            }
        }
    }

    private static void writePrivilegeNode(NodeUtil privilegesNode, PrivilegeDefinition definition) throws RepositoryException {
        String name = definition.getName();
        if (privilegesNode.hasChild(definition.getName())) {
            throw new RepositoryException("Privilege definition with name '"+name+"' already exists.");
        }

        NodeUtil privNode = privilegesNode.addChild(name, NT_REP_PRIVILEGE);
        if (definition.isAbstract()) {
            privNode.setBoolean(REP_IS_ABSTRACT, true);
        }
        Set<String> declAggrNames = definition.getDeclaredAggregateNames();
        if (!declAggrNames.isEmpty()) {
            String[] names = definition.getDeclaredAggregateNames().toArray(new String[declAggrNames.size()]);
            privNode.setNames(REP_AGGREGATES, names);
        }
    }
}