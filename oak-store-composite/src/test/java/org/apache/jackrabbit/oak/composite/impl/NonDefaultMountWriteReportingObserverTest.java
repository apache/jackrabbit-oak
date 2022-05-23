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
package org.apache.jackrabbit.oak.composite.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.composite.MountInfoPropsBuilder;
import org.apache.jackrabbit.oak.composite.MountInfoProviderService;
import org.apache.jackrabbit.oak.composite.impl.NonDefaultMountWriteReportingObserver.ChangeReporter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class NonDefaultMountWriteReportingObserverTest {
    
    @Rule
    public final OsgiContext context = new OsgiContext();

    private SpyChangeReporter reporter;
    private NonDefaultMountWriteReportingObserver observer;
    
    @Before
    public void prepare() {
        registerMountInfoProviderService();
        observer = context.registerInjectActivateService(new NonDefaultMountWriteReportingObserver(), "ignoredClassNameFragments", "MarkerToBeIgnored");
        reporter = new SpyChangeReporter();
        observer.setReporter(reporter);
    }

    @Test
    public void pathAddedUnderNonDefaultMount() throws CommitFailedException {
        
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        nodeStore.addObserver(observer);
        
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder
            .child("foo")
            .child("bar")
            .child("baz");
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertThat(reporter.changes, equalTo(Arrays.asList("Added|/foo/bar", "Added|/foo/bar/baz") ) );
    }

    @Test
    public void subPathAddedUnderNonDefaultMount() throws CommitFailedException {
        
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        nodeStore.addObserver(observer);
        
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder
            .child("foo")
            .child("bar");
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        reporter.changes.clear();

        NodeBuilder builder2 = nodeStore.getRoot().builder();
        builder2
            .child("foo")
            .child("bar")
            .child("baz");

        nodeStore.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertThat(reporter.changes, equalTo(Arrays.asList("Changed|/foo/bar", "Added|/foo/bar/baz") ) );
    }
    
    @Test
    public void propertyChangedUnderNonDefaultMount() throws CommitFailedException {
        
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        nodeStore.addObserver(observer);
        
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder
            .child("foo")
            .child("bar")
            .child("baz");
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        reporter.changes.clear();

        NodeBuilder builder2 = nodeStore.getRoot().builder();
        builder2
            .child("foo")
            .child("bar")
            .child("baz")
            .setProperty("prop", "val");

        nodeStore.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertThat(reporter.changes, equalTo(Arrays.asList("Changed|/foo/bar", "Changed|/foo/bar/baz")));
    }    
    
    @Test
    public void subPathAddedUnderDefaultMount() throws CommitFailedException {
        
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        nodeStore.addObserver(observer);
        
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder
            .child("baz");
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertThat(reporter.changes, equalTo(Arrays.asList() ) );
    }
    
    @Test
    public void subPathUnderNonDefaultMountButWithExpectedComponent() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        nodeStore.addObserver(observer);
        
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder
            .child("foo")
            .child("bar");
        
        MarkerToBeIgnored.call( () -> nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY));
        
        assertThat(reporter.changes, equalTo(Arrays.asList() ) );
    }
    
    @Test
    public void commitWithMixedDefaultAndNonDefaultMounts() throws CommitFailedException {

        MemoryNodeStore nodeStore = new MemoryNodeStore();
        nodeStore.addObserver(observer);
        
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder
            .child("foo")
            .child("bar");

        builder
            .child("outside");

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertThat(reporter.changes, equalTo(Arrays.asList("Added|/foo/bar")));
    }
    
    @Test
    public void commitWithDeletedNodeUnderNonDefaultMount() throws CommitFailedException {

        MemoryNodeStore nodeStore = new MemoryNodeStore();
        nodeStore.addObserver(observer);
        
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder
            .child("foo")
            .child("bar")
            .child("baz");
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        reporter.changes.clear();

        NodeBuilder builder2 = nodeStore.getRoot().builder();
        builder2
            .child("foo")
            .child("bar")
            .child("baz")
            .remove();

        nodeStore.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertThat(reporter.changes, equalTo(Arrays.asList("Changed|/foo/bar", "Deleted|/foo/bar/baz")));        
    }

    private void registerMountInfoProviderService() {
        MountInfoProviderService mountInfoProviderService = new MountInfoProviderService();
        context.registerService(mountInfoProviderService);
        mountInfoProviderService.activate(context.bundleContext(), new MountInfoPropsBuilder()
            .withMountPaths("/foo/bar")
            .buildProviderServiceProps());
    }
    
    static class SpyChangeReporter extends ChangeReporter {
        List<String> changes = new ArrayList<>();
        
        @Override
        void reportChanges(Map<String, String> changes, RuntimeException ignored) {
            changes.forEach( (path, type) -> this.changes.add(type + "|" + path) );
        }
    }
    
    static class MarkerToBeIgnored {
        
        static <T> void call(Callable<T> callable) throws Exception {
            callable.call();
        }
    }
    
}
