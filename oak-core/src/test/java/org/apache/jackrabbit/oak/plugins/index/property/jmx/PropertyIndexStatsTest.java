/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.property.jmx;

import java.util.HashSet;
import java.util.List;

import javax.management.openmbean.CompositeData;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.*;

public class PropertyIndexStatsTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new PropertyIndexEditorProvider()));

    private NodeStore store = new MemoryNodeStore();

    private PropertyIndexStats mbean = new PropertyIndexStats();

    @Test
    public void jmxReg() throws Exception{
        activateMBean();

        assertNotNull(context.getService(PropertyIndexStatsMBean.class));

        MockOsgi.deactivate(mbean, context.bundleContext());
        assertNull(context.getService(PropertyIndexStatsMBean.class));
    }


    @Test
    public void statsForSpecificIndex() throws Exception{
        prepareStore();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);

        setProperty(builder, "/a/b/c", "foo", "x");
        setProperty(builder, "/a/e/c/d", "foo", "y");

        store.merge(builder, HOOK, CommitInfo.EMPTY);

        CompositeData cd = mbean.getStatsForSpecificIndex("/oak:index/foo", 5, 2, 100);
        assertEquals(2L, cd.get("valueCount"));
        assertArray(cd, "values", asList("x", "y"));
        assertArray(cd, "paths", asList("/a/b", "/a/e"));

        cd = mbean.getStatsForSpecificIndex("/oak:index/foo", 5, 3, 100);
        assertArray(cd, "paths", asList("/a/b/c", "/a/e/c"));

        cd = mbean.getStatsForSpecificIndex("/oak:index/foo", 5, 5, 100);
        assertArray(cd, "paths", asList("/a/b/c", "/a/e/c/d"));
    }

    @Test
    public void statsForSpecificIndexWithPathLimit() throws Exception{
        prepareStore();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);

        for (int i = 0; i < 10; i++) {
            setProperty(builder, "/a/b/c/d" + i, "foo", "x");
        }

        store.merge(builder, HOOK, CommitInfo.EMPTY);

        CompositeData cd = mbean.getStatsForSpecificIndex("/oak:index/foo", 5, 7, 3);
        assertEquals(1L, cd.get("valueCount"));
        assertArray(cd, "values", asList("x"));
        assertArray(cd, "paths", asList("/a/b/c"));
    }

    private static void assertArray(CompositeData cd, String prop, List<String> values){
        String[] a = (String[])cd.get(prop);
        assertEquals(new HashSet<String>(values), new HashSet<String>(Lists.newArrayList(a)));
    }

    private static void setProperty(NodeBuilder builder, String path, String name, String value){
        for (String p : PathUtils.elements(path)){
            builder = builder.child(p);
        }
        builder.setProperty(name, value);
    }

    private void prepareStore() throws CommitFailedException {
        activateMBean();
        NodeState root = store.getRoot();

        NodeBuilder builder = root.builder();
        new InitialContent().initialize(builder);

        store.merge(builder, HOOK, CommitInfo.EMPTY);
    }

    private void activateMBean() {
        context.registerService(NodeStore.class, store);
        context.registerInjectActivateService(mbean);
    }

}