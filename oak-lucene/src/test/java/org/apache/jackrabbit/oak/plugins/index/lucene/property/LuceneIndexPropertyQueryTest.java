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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.child;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

public class LuceneIndexPropertyQueryTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();
    private IndexTracker tracker = new IndexTracker();

    private String indexPath  = "/oak:index/foo";
    private IndexDefinitionBuilder defnb = new IndexDefinitionBuilder(child(builder, indexPath));

    private LuceneIndexPropertyQuery query = new LuceneIndexPropertyQuery(tracker, indexPath);

    @Test
    public void simplePropertyIndex() throws Exception{
        defnb.noAsync();
        defnb.indexRule("nt:base").property("foo").propertyIndex();

        assertEquals(0,Iterables.size(query.getIndexedPaths("foo", "bar")));

        NodeState before = builder.getNodeState();
        builder.child("a").setProperty("foo", "bar");
        builder.child("b").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        EditorHook hook = new EditorHook(new IndexUpdateProvider(new LuceneIndexEditorProvider()));
        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertThat(query.getIndexedPaths("foo", "bar"),
                containsInAnyOrder("/a", "/b"));
    }
}