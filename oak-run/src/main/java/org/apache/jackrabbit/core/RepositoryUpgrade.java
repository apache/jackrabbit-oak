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
package org.apache.jackrabbit.core;

import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.io.File;
import java.io.IOException;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.RepositoryImpl.WorkspaceInfo;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.core.nodetype.NodeTypeRegistry;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.QNodeTypeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryUpgrade {

    /**
     * Logger instance
     */
    private static final Logger logger =
        LoggerFactory.getLogger(RepositoryUpgrade.class);

    /**
     * Source repository context.
     */
    private final RepositoryContext source;

    /**
     * Target node store.
     */
    private final NodeStore target;

    /**
     * Copies the contents of the repository in the given source directory
     * to the given target node store.
     *
     * @param source source repository directory
     * @param target target node store
     * @throws RepositoryException if the copy operation fails
     */
    public static void copy(File source, NodeStore target)
            throws RepositoryException {
        copy(RepositoryConfig.create(source), target);
    }

    /**
     * Copies the contents of the repository with the given configuration
     * to the given target node builder.
     *
     * @param source source repository configuration
     * @param target target node store
     * @throws RepositoryException if the copy operation fails
     */
    public static void copy(RepositoryConfig source, NodeStore target)
            throws RepositoryException {
        RepositoryImpl repository = RepositoryImpl.create(source);
        try {
            copy(repository, target);
        } finally {
            repository.shutdown();
        }
    }

    /**
     * Copies the contents of the given source repository to the given
     * target node store.
     * <p>
     * The source repository <strong>must not be modified</strong> while
     * the copy operation is running to avoid an inconsistent copy.
     *
     * @param source source repository directory
     * @param target target node store
     * @throws RepositoryException if the copy operation fails
     * @throws IOException if the target repository can not be initialized
     */
    public static void copy(RepositoryImpl source, NodeStore target)
            throws RepositoryException {
        new RepositoryUpgrade(source, target).copy();
    }

    /**
     * Creates a tool for copying the full contents of the source repository
     * to the given target repository. Any existing content in the target
     * repository will be overwritten.
     *
     * @param source source repository
     * @param target target node store
     */
    public RepositoryUpgrade(RepositoryImpl source, NodeStore target) {
        this.source = source.getRepositoryContext();
        this.target = target;
    }

    /**
     * Copies the full content from the source to the target repository.
     * <p>
     * The source repository <strong>must not be modified</strong> while
     * the copy operation is running to avoid an inconsistent copy.
     * <p>
     * This method leaves the search indexes of the target repository in
     * an 
     * Note that both the source and the target repository must be closed
     * during the copy operation as this method requires exclusive access
     * to the repositories.
     *
     * @throws RepositoryException if the copy operation fails
     */
    public void copy() throws RepositoryException {
        logger.info(
                "Copying repository content from {} to Oak",
                source.getRepository().repConfig.getHomeDir());
        try {
            NodeStoreBranch branch = target.branch();
            NodeBuilder builder = branch.getHead().builder();

            copyNamespaces(builder);
            copyNodeTypes(builder);
            copyVersionStore(builder);
            copyWorkspaces(builder);

            branch.setRoot(builder.getNodeState());
            branch.merge(EmptyHook.INSTANCE); // TODO: default hooks?
        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private String getOakName(Name name) throws NamespaceException {
        String uri = name.getNamespaceURI();
        String local = name.getLocalName();
        if (uri == null || uri.isEmpty()) {
            return local;
        } else {
            return source.getNamespaceRegistry().getPrefix(uri) + ":" + local;
        }
    }

    private void copyNamespaces(NodeBuilder root) throws RepositoryException {
        NamespaceRegistry sourceRegistry = source.getNamespaceRegistry();
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder namespaces = system.child("rep:namespaces");

        logger.info("Copying registered namespaces");
        for (String uri : sourceRegistry.getURIs()) {
            namespaces.setProperty(sourceRegistry.getPrefix(uri), uri);
        }
    }

    private void copyNodeTypes(NodeBuilder root) throws RepositoryException {
        NodeTypeRegistry sourceRegistry = source.getNodeTypeRegistry();
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder types = system.child(JCR_NODE_TYPES);

        logger.info("Copying registered node types");
        for (Name name : sourceRegistry.getRegisteredNodeTypes()) {
            QNodeTypeDefinition def = sourceRegistry.getNodeTypeDef(name);
            NodeBuilder type = types.child(getOakName(name));
            type.setProperty(JCR_NODETYPENAME, getOakName(name));
            // TODO ...
        }
    }

    private void copyVersionStore(NodeBuilder root)
            throws RepositoryException, IOException {
        logger.info("Copying version histories");
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder versionStorage = system.child(JCR_VERSIONSTORAGE);
        NodeBuilder activities = system.child("rep:activities");

        PersistenceCopier copier = new PersistenceCopier(
                source.getInternalVersionManager().getPersistenceManager(),
                source.getNamespaceRegistry(), target);
        copier.copy(RepositoryImpl.VERSION_STORAGE_NODE_ID, versionStorage);
        copier.copy(RepositoryImpl.ACTIVITIES_NODE_ID, activities);
    }   

    private void copyWorkspaces(NodeBuilder root)
            throws RepositoryException, IOException {
        logger.info("Copying default workspace");

        // Copy all the default workspace content

        RepositoryImpl repository = source.getRepository();
        RepositoryConfig config = repository.getConfig();
        String name = config.getDefaultWorkspaceName();
        WorkspaceInfo workspace = repository.getWorkspaceInfo(name);

        PersistenceCopier copier = new PersistenceCopier(
                workspace.getPersistenceManager(),
                source.getNamespaceRegistry(), target);
        copier.excludeNode(RepositoryImpl.SYSTEM_ROOT_NODE_ID);
        copier.copy(RepositoryImpl.ROOT_NODE_ID, root);

        // TODO: Copy all the active open-scoped locks
    }


}
