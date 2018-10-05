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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;

import java.util.function.Consumer;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.composite.checks.UniqueIndexNodeStoreChecker.Context;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableSet;

public class UniqueIndexNodeStoreCheckerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    private MountInfoProvider mip;
    
    @Before
    public void prepare() {
        mip = Mounts.newBuilder().
                readOnlyMount("libs", "/libs", "/libs2").
                readOnlyMount("apps", "/apps", "/apps2").
                build();
    }
    
    @Test
    public void uuidConflict_twoStores() throws Exception {
        
        MemoryNodeStore globalStore = new MemoryNodeStore();
        MemoryNodeStore mountedStore = new MemoryNodeStore();
        
        populateStore(globalStore, b  -> b.child("first").setProperty("foo", "bar"));
        populateStore(mountedStore, b -> b.child("libs").child("first").setProperty("foo", "bar"));
        
        UniqueIndexNodeStoreChecker checker = new UniqueIndexNodeStoreChecker();
        Context ctx = checker.createContext(globalStore, mip);
        
        exception.expect(IllegalRepositoryStateException.class);
        exception.expectMessage("1 errors were found");
        exception.expectMessage("clash for value bar: 'duplicate unique index entry'");
        
        ErrorHolder error = new ErrorHolder();
        checker.check(new MountedNodeStore(mip.getMountByName("libs"), mountedStore), TreeFactory.createReadOnlyTree(mountedStore.getRoot()), error, ctx);
        error.end();
    }
    
    @Test
    public void uuidConflict_threeStores() throws Exception {
        
        MemoryNodeStore globalStore = new MemoryNodeStore();
        MemoryNodeStore mountedStore = new MemoryNodeStore();
        MemoryNodeStore mountedStore2 = new MemoryNodeStore();
        
        populateStore(globalStore, b  -> b.child("first").setProperty("foo", "bar"));
        populateStore(globalStore, b  -> b.child("second").setProperty("foo", "baz"));
        populateStore(mountedStore, b -> b.child("libs").child("first").setProperty("foo", "bar"));
        populateStore(mountedStore2, b -> b.child("apps").child("first").setProperty("foo", "baz"));
        
        UniqueIndexNodeStoreChecker checker = new UniqueIndexNodeStoreChecker();
        Context ctx = checker.createContext(globalStore, mip);
        
        exception.expect(IllegalRepositoryStateException.class);
        exception.expectMessage("2 errors were found");
        exception.expectMessage("clash for value bar: 'duplicate unique index entry'");
        exception.expectMessage("clash for value baz: 'duplicate unique index entry'");
        
        ErrorHolder error = new ErrorHolder();
        checker.check(new MountedNodeStore(mip.getMountByName("libs"), mountedStore), TreeFactory.createReadOnlyTree(mountedStore.getRoot()), error, ctx);
        checker.check(new MountedNodeStore(mip.getMountByName("apps"), mountedStore2), TreeFactory.createReadOnlyTree(mountedStore.getRoot()), error, ctx);
        error.end();
    }
    
    @Test
    public void noConflict() throws Exception {

        MemoryNodeStore globalStore = new MemoryNodeStore();
        MemoryNodeStore mountedStore = new MemoryNodeStore();
        
        populateStore(globalStore, b  -> b.child("first").setProperty("foo", "baz"));
        populateStore(mountedStore, b -> b.child("libs").child("first").setProperty("foo", "bar"));
        
        UniqueIndexNodeStoreChecker checker = new UniqueIndexNodeStoreChecker();
        Context ctx = checker.createContext(globalStore, mip);
        
        ErrorHolder error = new ErrorHolder();
        checker.check(new MountedNodeStore(mip.getMountByName("libs"), mountedStore), TreeFactory.createReadOnlyTree(mountedStore.getRoot()), error, ctx);
        error.end();
    }
    
    /**
     * Tests that if a mount has an index clash but the path does not belong to the mount no error is reported
     */
    @Test
    public void noConflict_mountHasDuplicateOutsideOfPath() throws Exception {
        
        MemoryNodeStore globalStore = new MemoryNodeStore();
        MemoryNodeStore mountedStore = new MemoryNodeStore();
        
        populateStore(globalStore, b  -> b.child("first").setProperty("foo", "bar"));
        populateStore(mountedStore, b -> b.child("second").setProperty("foo", "bar"));
        
        UniqueIndexNodeStoreChecker checker = new UniqueIndexNodeStoreChecker();
        Context ctx = checker.createContext(globalStore, mip);
        
        ErrorHolder error = new ErrorHolder();
        checker.check(new MountedNodeStore(mip.getMountByName("libs"), mountedStore), TreeFactory.createReadOnlyTree(mountedStore.getRoot()), error, ctx);
        error.end();
    }
    
    /**
     * Tests that if a mount has an index clash but the path does not belog to the global mount no error is reported  
     * 
     */
    @Test
    public void noConflict_globalMountHasDuplicateOutsideOfPath() throws Exception {
        
        MemoryNodeStore globalStore = new MemoryNodeStore();
        MemoryNodeStore mountedStore = new MemoryNodeStore();
        
        populateStore(globalStore, b  -> b.child("libs").child("first").setProperty("foo", "bar"));
        populateStore(mountedStore, b -> b.child("libs").child("second").setProperty("foo", "bar"));
        
        UniqueIndexNodeStoreChecker checker = new UniqueIndexNodeStoreChecker();
        Context ctx = checker.createContext(globalStore, mip);
        
        ErrorHolder error = new ErrorHolder();
        checker.check(new MountedNodeStore(mip.getMountByName("libs"), mountedStore), TreeFactory.createReadOnlyTree(mountedStore.getRoot()), error, ctx);
        error.end();
    }
    
    private void populateStore(NodeStore ns, Consumer<NodeBuilder> action) throws CommitFailedException {
        
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, true, ImmutableSet.of("foo"), null);
        index.setProperty("entryCount", -1);   
        
        action.accept(builder);
        
        ns.merge(builder,new EditorHook(new IndexUpdateProvider(
                new PropertyIndexEditorProvider().with(mip))), CommitInfo.EMPTY);
    }
}
