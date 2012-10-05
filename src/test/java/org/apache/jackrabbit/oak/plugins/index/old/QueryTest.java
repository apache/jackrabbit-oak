/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.old;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.index.IndexWrapper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the query feature.
 */
public class QueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {

        // the property and prefix index currently require the index wrapper
        IndexWrapper mk = new IndexWrapper(new MicroKernelImpl(),
                PathUtils.concat(TEST_INDEX_HOME, TEST_INDEX_NAME));
        Indexer indexer = mk.getIndexer();

        // MicroKernel mk = new MicroKernelImpl();
        // Indexer indexer = new Indexer(mk);

        PropertyIndexer pi = new PropertyIndexer(indexer);
        QueryIndexProvider qip = new CompositeQueryIndexProvider(pi);
        CompositeHook hook = new CompositeHook(pi);
        createDefaultKernelTracker().available(mk);
        return new Oak(mk).with(qip).with(hook).createContentRepository();
    }

    @Test
    public void sql2Explain() throws Exception {
        test("sql2_explain.txt");
    }

    @Test
    @Ignore("OAK-288 prevents the index from seeing updates that happened directly on the mk")
    public void sql2() throws Exception {
        test("sql2.txt");
    }

}
