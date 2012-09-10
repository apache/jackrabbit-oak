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
package org.apache.jackrabbit.oak.plugins.lucene;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.spi.query.IndexUtils.DEFAULT_INDEX_HOME;

import javax.security.auth.Subject;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryValueFactory;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexDefinitionImpl;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.junit.Test;

public class LuceneEditorTest implements LuceneIndexConstants {

    @Test
    public void testLucene() throws Exception {
        IndexDefinition testID = new IndexDefinitionImpl(DEFAULT_INDEX_NAME,
                TYPE, DEFAULT_INDEX_HOME, false, null);

        KernelNodeStore store = new KernelNodeStore(new MicroKernelImpl());
        store.setHook(new LuceneEditor(testID));
        Root root = new RootImpl(store, null, new Subject());
        Tree tree = root.getTree("/");

        tree.setProperty("foo", MemoryValueFactory.INSTANCE.createValue("bar"));
        root.commit(DefaultConflictHandler.OURS);

        QueryIndex index = new LuceneIndex(store, testID);
        FilterImpl filter = new FilterImpl(null);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                MemoryValueFactory.INSTANCE.createValue("bar"));
        Cursor cursor = index.query(filter, null, store.getRoot());
        assertTrue(cursor.next());
        assertEquals("/", cursor.currentRow().getPath());
        assertFalse(cursor.next());
    }

}
