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
package org.apache.jackrabbit.oak.composite.checks;

import java.io.IOException;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.composite.checks.NodeTypeDefinitionNodeStoreChecker.Context;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

/**
 * Validates that the <tt>NodeTypeDefinitionNodeStoreChecker</tt> is properly applied
 * 
 * <p>This class does not attempt to exhaustively validate the checks that should be performed, only
 * that they are performed when needed.</p>
 *
 */
@Component
@Service(MountedNodeStoreChecker.class)
public class NodeTypeDefinitionNodeStoreCheckerTest {

    @Test
    public void mountWithConsistentDefinition() throws CommitFailedException, IOException {
        
        new Fixture() {
            @Override
            protected void initMountContent(NodeBuilder builder) {
                builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_ROOT, Type.NAME);
                builder.child("first")
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_UNSTRUCTURED, Type.NAME);
            }
        }.run();
    }
    
    @Test(expected = IllegalRepositoryStateException.class)
    public void mountWithInconsistentDefinition() throws CommitFailedException {
        
        new Fixture() {
            @Override
            protected void initMountContent(NodeBuilder builder) {
                builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, "nt:missing", Type.NAME);
            }
        }.run();
    }

    @Test(expected = IllegalRepositoryStateException.class)
    public void addingDefaultPrimaryTypeFails() throws CommitFailedException {
        
        new Fixture() {
            @Override
            protected void initMountContent(NodeBuilder builder) {
                builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, "nt:missing", Type.NAME);
                // missing primary type - would try to add default one, which fails due to the NodeBuilder
                // being read-only
                builder.child("first");
            }
        }.run();
    }
    
    @Test
    public void mountWithDefinitionButHiddenNodes() throws CommitFailedException {
        
        new Fixture() {
            @Override
            protected void initMountContent(NodeBuilder builder) {
                builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_ROOT, Type.NAME);
                // if visible would try to add a default node type - which would fail 
                builder.child(":hidden").setProperty("foo", "bar");
            }
        }.run();
    }
    
    static abstract class Fixture {
        
        public void run() throws CommitFailedException {
            MemoryNodeStore root = new MemoryNodeStore();
            MemoryNodeStore mount = new MemoryNodeStore();
            
            NodeBuilder rootBuilder = root.getRoot().builder();
            new InitialContent().initialize(rootBuilder);
            root.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            
            NodeBuilder mountBuilder = mount.getRoot().builder();
            initMountContent(mountBuilder);
            mount.merge(mountBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            
            MountInfoProvider mip = Mounts.newBuilder()
                    .readOnlyMount("first", "/first")
                    .build();
            
            NodeTypeDefinitionNodeStoreChecker checker = new NodeTypeDefinitionNodeStoreChecker();
            Context context = checker.createContext(root, mip);
            ErrorHolder errorHolder = new ErrorHolder();
            
            checker.check(new MountedNodeStore(mip.getMountByName("first"), mount), TreeFactory.createReadOnlyTree(mount.getRoot()), errorHolder, context);
            errorHolder.end();
        }
        
        protected abstract void initMountContent(NodeBuilder builder);
    }
}
