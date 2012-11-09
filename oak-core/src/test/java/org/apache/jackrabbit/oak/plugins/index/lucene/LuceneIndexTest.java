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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class LuceneIndexTest implements LuceneIndexConstants {

    @Test
    public void testLucene() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("lucene")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE)
                .setProperty("type", TYPE_LUCENE);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        IndexHook l = new LuceneIndexDiff(builder);
        after.compareAgainstBaseState(before, l);
        l.apply();
        l.close();

        IndexDefinition testDef = new IndexDefinitionImpl("lucene",
                TYPE_LUCENE, "/oak:index/lucene");
        QueryIndex queryIndex = new LuceneIndex(testDef);
        FilterImpl filter = new FilterImpl(null);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(filter, builder.getNodeState());
        assertTrue(cursor.next());
        assertEquals("/", cursor.currentRow().getPath());
        assertFalse(cursor.next());
    }

}
