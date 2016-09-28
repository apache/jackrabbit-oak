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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of the {@code RepositoryInitializer} interface responsible for
 * setting up query indices for the system maintained, protected properties defined
 * by this module:
 *
 * <ul>
 *     <li>Index Definition <i>externalPrincipalNames</i>: Indexing
 *     {@link ExternalIdentityConstants#REP_EXTERNAL_PRINCIPAL_NAMES} properties.
 *     This index is used by the {@link ExternalGroupPrincipalProvider} to lookup
 *     and find principals stored in this property.</li>
 * </ul>
 *
 * @since Oak 1.5.3
 */
class ExternalIdentityRepositoryInitializer implements RepositoryInitializer {

    private static final Logger log = LoggerFactory.getLogger(ExternalIdentityRepositoryInitializer.class);

    private final boolean enforceUniqueIds;

    ExternalIdentityRepositoryInitializer(boolean enforceUniqueIds) {
        this.enforceUniqueIds = enforceUniqueIds;
    }

    @Override
    public void initialize(@Nonnull NodeBuilder builder) {
        NodeState base = builder.getNodeState();
        NodeStore store = new MemoryNodeStore(base);

        String errorMsg = "Failed to initialize external identity content.";
        try {

            Root root = RootFactory.createSystemRoot(store,
                    new EditorHook(new CompositeEditorProvider(new NamespaceEditorProvider(), new TypeEditorProvider())),
                    null, null, null, null);

            // create index definition for "rep:externalId" and "rep:externalPrincipalNames"
            NodeUtil rootTree = checkNotNull(new NodeUtil(root.getTree("/")));
            NodeUtil index = rootTree.getOrAddChild(IndexConstants.INDEX_DEFINITIONS_NAME, JcrConstants.NT_UNSTRUCTURED);

            if (enforceUniqueIds && !index.hasChild("externalId")) {
                NodeUtil definition = IndexUtils.createIndexDefinition(index, "externalId", true,
                        new String[]{ExternalIdentityConstants.REP_EXTERNAL_ID}, null);
                definition.setString("info", "Oak index assuring uniqueness of rep:externalId properties.");
            }

            if (!index.hasChild("externalPrincipalNames")) {
                NodeUtil definition = IndexUtils.createIndexDefinition(index, "externalPrincipalNames", false,
                        new String[]{ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES}, null);
                definition.setString("info", "Oak index used by the principal management provided by the external authentication module.");
            }

            if (root.hasPendingChanges()) {
                root.commit();
            }
        } catch (RepositoryException e) {
            log.error(errorMsg, e);
            throw new RuntimeException(e);
        } catch (CommitFailedException e) {
            log.error(errorMsg, e);
            throw new RuntimeException(e);
        }

        NodeState target = store.getRoot();
        target.compareAgainstBaseState(base, new ApplyDiff(builder));
    }
}