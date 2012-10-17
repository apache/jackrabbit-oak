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
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.lifecycle.DefaultMicroKernelTracker;
import org.apache.jackrabbit.oak.spi.lifecycle.MicroKernelTracker;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

/**
 * {@code InitialContent} implements a {@link MicroKernelTracker} and
 * registers built-in node types when the micro kernel becomes available.
 */
@Component
@Service(MicroKernelTracker.class)
public class InitialContent extends DefaultMicroKernelTracker {

    @Override
    public void available(MicroKernel mk) {
        NodeStore store = new Oak(mk).createNodeStore();
        NodeStoreBranch branch = store.branch();

        NodeBuilder root = branch.getRoot().builder();
        root.setProperty("jcr:primaryType", "rep:root", Type.NAME);

        if (!root.hasChildNode("jcr:system")) {
            NodeBuilder system = root.child("jcr:system");
            system.setProperty("jcr:primaryType", "rep:system", Type.NAME);

            system.child("jcr:versionStorage")
                .setProperty("jcr:primaryType", "rep:versionStorage", Type.NAME);
            system.child("jcr:nodeTypes")
                .setProperty("jcr:primaryType", "rep:nodeTypes", Type.NAME);
            system.child("jcr:activities")
                .setProperty("jcr:primaryType", "rep:Activities", Type.NAME);
            system.child("rep:privileges")
                .setProperty("jcr:primaryType", "rep:Privileges", Type.NAME);

            NodeBuilder security = root.child("rep:security");
            security.setProperty(
                    "jcr:primaryType", "rep:AuthorizableFolder", Type.NAME);

            NodeBuilder authorizables = security.child("rep:authorizables");
            authorizables.setProperty(
                    "jcr:primaryType", "rep:AuthorizableFolder", Type.NAME);

            NodeBuilder users = authorizables.child("rep:users");
            users.setProperty(
                    "jcr:primaryType", "rep:AuthorizableFolder", Type.NAME);

            NodeBuilder a = users.child("a");
            a.setProperty("jcr:primaryType", "rep:AuthorizableFolder", Type.NAME);

            a.child("ad")
                .setProperty("jcr:primaryType", "rep:AuthorizableFolder", Type.NAME)
                .child("admin")
                .setProperty("jcr:primaryType", "rep:User", Type.NAME)
                .setProperty("jcr:uuid", "21232f29-7a57-35a7-8389-4a0e4a801fc3")
                .setProperty("rep:principalName", "admin")
                .setProperty("rep:authorizableId", "admin")
                .setProperty("rep:password", "{SHA-256}9e515755e95513ce-1000-0696716f8baf8890a35eda1b9f2d5a4e727d1c7e1c062f03180dcc2a20f61f3b");

            a.child("an")
                .setProperty("jcr:primaryType", "rep:AuthorizableFolder", Type.NAME)
                .child("anonymous")
                .setProperty("jcr:primaryType", "rep:User", Type.NAME)
                .setProperty("jcr:uuid", "294de355-7d9d-30b3-92d8-a1e6aab028cf")
                .setProperty("rep:principalName", "anonymous")
                .setProperty("rep:authorizableId", "anonymous");
        }

        if (!root.hasChildNode("oak:index")) {
            NodeBuilder index = root.child("oak:index");
            index.child("jcr:uuid")
                .setProperty("jcr:pimaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("unique", true);
            // FIXME: user-mgt related unique properties (rep:authorizableId, rep:principalName) are implementation detail and not generic for repo
            // FIXME: rep:principalName only needs to be unique if defined with user/group nodes -> add defining nt-info to uniqueness constraint otherwise ac-editing will fail.
            index.child("rep:authorizableId")
                .setProperty("jcr:pimaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("unique", true);
            index.child("rep:principalName")
                .setProperty("jcr:pimaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("unique", true);
        }
        try {
            branch.setRoot(root.getNodeState());
            branch.merge();
        } catch (CommitFailedException e) {
            throw new RuntimeException(e); // TODO: shouldn't need the wrapper
        }

        BuiltInNodeTypes.register(createRoot(mk));
    }

    private Root createRoot(MicroKernel mk) {
        Oak oak = new Oak(mk);
        oak.with(new OpenSecurityProvider()); // TODO: this shouldn't be needed
        try {
            return oak.createContentRepository().login(null, null).getLatestRoot();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create a Root", e);
        }
    }
}
