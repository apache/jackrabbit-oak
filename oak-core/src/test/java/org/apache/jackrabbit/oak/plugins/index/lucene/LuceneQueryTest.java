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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static org.apache.jackrabbit.oak.spi.query.IndexUtils.DEFAULT_INDEX_HOME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.Before;
import org.junit.Test;

/**
 * base class for lucene search tests
 */
public class LuceneQueryTest extends AbstractQueryTest implements
        LuceneIndexConstants {

    protected static final String SQL2 = "JCR-SQL2";

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        createIndexNode();
    }

    @Override
    protected ContentRepository createRepository() {
        QueryIndexProvider qip = new CompositeQueryIndexProvider(
                new LuceneIndexProvider(DEFAULT_INDEX_HOME));
        CommitHook ch = new CompositeHook(new LuceneReindexHook(
                DEFAULT_INDEX_HOME), new LuceneHook(DEFAULT_INDEX_HOME));
        MicroKernel mk = new MicroKernelImpl();
        createDefaultKernelTracker().available(mk);
        return new Oak(mk).with(qip).with(ch).createContentRepository();
    }

    protected void createIndexNode() throws Exception {
        Tree index = root.getTree("/");
        for (String p : PathUtils.elements(DEFAULT_INDEX_HOME)) {
            if (index.hasChild(p)) {
                index = index.getChild(p);
            } else {
                index = index.addChild(p);
            }
        }
        index.addChild("test-lucene").setProperty("type",
                vf.createValue("lucene"));
        root.commit();
    }

    @Test
    public void simpleSql2() throws Exception {

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", vf.createValue("hello"));
        test.addChild("b").setProperty("name", vf.createValue("nothello"));
        root.commit();

        String sql = "select * from [nt:base] where name = 'hello'";

        Iterator<? extends ResultRow> result;
        result = executeQuery(sql, SQL2, null).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next().getPath());
        assertFalse(result.hasNext());
    }

}