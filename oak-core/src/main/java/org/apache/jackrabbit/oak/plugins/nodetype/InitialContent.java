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
package org.apache.jackrabbit.oak.plugins.nodetype;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import com.google.common.collect.ImmutableList;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;

/**
 * {@code InitialContent} implements a {@link RepositoryInitializer} and
 * registers built-in node types when the micro kernel becomes available.
 */
@Component
@Service(RepositoryInitializer.class)
public class InitialContent implements RepositoryInitializer {

    @Override
    public void initialize(NodeStore store) {
        NodeStoreBranch branch = store.branch();

        NodeBuilder root = branch.getRoot().builder();
        root.setProperty(JCR_PRIMARYTYPE, "rep:root", Type.NAME);

        if (!root.hasChildNode(JCR_SYSTEM)) {
            NodeBuilder system = root.child(JCR_SYSTEM);
            system.setProperty(JCR_PRIMARYTYPE, "rep:system", Type.NAME);

            system.child(JCR_VERSIONSTORAGE)
                .setProperty(JCR_PRIMARYTYPE, "rep:versionStorage", Type.NAME);
            system.child("jcr:nodeTypes")
                .setProperty(JCR_PRIMARYTYPE, "rep:nodeTypes", Type.NAME);
            system.child("jcr:activities")
                .setProperty(JCR_PRIMARYTYPE, "rep:Activities", Type.NAME);
        }

        if (!root.hasChildNode("oak:index")) {
            NodeBuilder index = root.child("oak:index");
            index.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED); // TODO: use proper node type
            index.child("uuid")
                .setProperty(JCR_PRIMARYTYPE, "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "property")
                .setProperty("propertyNames", "jcr:uuid")
                .setProperty("reindex", true)
                .setProperty("unique", true);
            index.child("nodetype")
                .setProperty(JCR_PRIMARYTYPE, "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "property")
                .setProperty("reindex", true)
                .setProperty(PropertyStates.createProperty(
                        "propertyNames",
                        ImmutableList.of(JCR_PRIMARYTYPE, JCR_MIXINTYPES),
                        Type.STRINGS));
            // FIXME: user-mgt related unique properties (rep:authorizableId, rep:principalName) are implementation detail and not generic for repo
            // FIXME OAK-396: rep:principalName only needs to be unique if defined with user/group nodes -> add defining nt-info to uniqueness constraint otherwise ac-editing will fail.
            index.child("authorizableId")
                .setProperty(JCR_PRIMARYTYPE, "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "property")
                .setProperty("propertyNames", UserConstants.REP_AUTHORIZABLE_ID)
                .setProperty("reindex", true)
                .setProperty("unique", true);
            index.child("principalName")
                .setProperty(JCR_PRIMARYTYPE, "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "property")
                .setProperty("propertyNames", UserConstants.REP_PRINCIPAL_NAME)
                .setProperty("reindex", true)
                .setProperty("unique", true);
            index.child("members")
                .setProperty(JCR_PRIMARYTYPE, "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "property")
                .setProperty("propertyNames", UserConstants.REP_MEMBERS)
                .setProperty("reindex", true);
        }
        try {
            branch.setRoot(root.getNodeState());
            branch.merge();
        } catch (CommitFailedException e) {
            throw new RuntimeException(e); // TODO: shouldn't need the wrapper
        }

        BuiltInNodeTypes.register(new RootImpl(store));
    }

}
