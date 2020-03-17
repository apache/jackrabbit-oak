/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite.checks;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.composite.checks.NamespacePrefixNodestoreChecker.Context;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NamespacePrefixNodestoreCheckerTest {
    
    @Rule
    public ExpectedException exception = ExpectedException.none();
    
    private MemoryNodeStore mount;
    private NamespacePrefixNodestoreChecker checker;
    private Context context;
    private MountInfoProvider mip;
    
    @Before
    public void prepareRepository() throws Exception {
        MemoryNodeStore root = new MemoryNodeStore();
        mount = new MemoryNodeStore();
        
        NodeBuilder rootBuilder = root.getRoot().builder();
        new InitialContent().initialize(rootBuilder);
        root.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        mip = Mounts.newBuilder()
                .readOnlyMount("first", "/first")
                .build();
        
        checker = new NamespacePrefixNodestoreChecker();
        context = checker.createContext(root, mip);
    }

    @Test
    public void invalidNamespacePrefix_node() throws Exception {
        
        NodeBuilder builder = mount.getRoot().builder();
        builder.child("libs").child("foo:first");
        
        mount.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        exception.expect(IllegalRepositoryStateException.class);
        exception.expectMessage("1 errors were found");
        exception.expectMessage("invalid namespace prefix foo");
        
        check("/libs/foo:first");
    }
    
    private void check(String path) {
        
        
        Tree tree = TreeUtil.getTree(TreeFactory.createReadOnlyTree(mount.getRoot()), path);
        
        ErrorHolder errorHolder = new ErrorHolder();
        checker.check(new MountedNodeStore(mip.getMountByName("first"), mount), tree, errorHolder, context);
        errorHolder.end();
    }
    
    @Test
    public void invalidNamespacePrefix_property() throws Exception {
        
        NodeBuilder builder = mount.getRoot().builder();
        builder.child("libs").setProperty("foo:prop", "value");
        mount.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        exception.expect(IllegalRepositoryStateException.class);
        exception.expectMessage("1 errors were found");
        exception.expectMessage("invalid namespace prefix foo");
        
        check("/libs");
    }
    
    @Test
    public void validNamespacePrefix() throws Exception {
        
        NodeBuilder builder = mount.getRoot().builder();
        builder.child("libs").setProperty("jcr:prop", "value");
        mount.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        check("/libs/jcr:prop");
    }
    
    @Test
    public void noNamespacePrefix() throws Exception {
        
        NodeBuilder builder = mount.getRoot().builder();
        builder.child("libs").setProperty("prop", "value");
        mount.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        check("/libs");
        
    }
}
