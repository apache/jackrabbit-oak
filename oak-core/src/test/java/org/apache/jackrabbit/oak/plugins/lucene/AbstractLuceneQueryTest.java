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
package org.apache.jackrabbit.oak.plugins.lucene;

import static org.apache.jackrabbit.oak.spi.query.IndexUtils.DEFAULT_INDEX_HOME;

import java.text.ParseException;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.Before;

/**
 * base class for lucene search tests
 */
public abstract class AbstractLuceneQueryTest extends AbstractOakTest implements
        LuceneIndexConstants {

    protected static final String SQL2 = "JCR-SQL2";

    protected ContentSession session;
    protected CoreValueFactory vf;
    protected SessionQueryEngine qe;
    protected Root root;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        session = createAdminSession();
        root = session.getLatestRoot();
        vf = session.getCoreValueFactory();
        qe = session.getQueryEngine();
        createIndexNode();
    }

    @Override
    protected ContentRepository createRepository() {
        QueryIndexProvider indexer = new LuceneIndexProvider(DEFAULT_INDEX_HOME);
        QueryIndexProvider qip = new CompositeQueryIndexProvider(indexer);
        return new ContentRepositoryImpl(new MicroKernelImpl(), qip,
                new LuceneHook(DEFAULT_INDEX_HOME));
    }

    private void createIndexNode() throws Exception {
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
        root.commit(DefaultConflictHandler.OURS);
    }

    protected Result executeQuery(String statement) throws ParseException {
        return qe.executeQuery(statement, SQL2, Long.MAX_VALUE, 0, null,
                session.getLatestRoot(), null);
    }
}