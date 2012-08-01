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

import static org.apache.jackrabbit.oak.plugins.lucene.LuceneIndexUtils.DEFAULT_INDEX_HOME;
import static org.apache.jackrabbit.oak.plugins.lucene.LuceneIndexUtils.DEFAULT_INDEX_NAME;
import static org.apache.jackrabbit.oak.plugins.lucene.LuceneIndexUtils.createIndexNode;

import java.text.ParseException;

import javax.jcr.GuestCredentials;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.junit.Before;

/**
 * base class for lucene search tests
 */
public abstract class AbstractLuceneQueryTest {

    private static String TEST_INDEX_NAME = DEFAULT_INDEX_NAME;

    protected static String SQL2 = "JCR-SQL2";

    protected ContentRepository repository;
    protected ContentSession session;
    protected CoreValueFactory vf;
    protected QueryEngine qe;
    protected Root root;

    @Before
    public void before() throws Exception {
        repository = new ContentRepositoryImpl(new MicroKernelImpl(),
                new LuceneIndexProvider(DEFAULT_INDEX_HOME), null);
        session = repository.login(new GuestCredentials(), null);
        cleanupIndexNode();

        vf = session.getCoreValueFactory();
        qe = session.getQueryEngine();

    }

    /**
     * Recreates an empty index node, ready to be used in tests
     * 
     * @throws Exception
     */
    private void cleanupIndexNode() throws Exception {
        root = session.getCurrentRoot();
        Tree index = root.getTree(DEFAULT_INDEX_HOME);
        if (index != null) {
            index = index.getChild(TEST_INDEX_NAME);
            if (index != null) {
                index.remove();
            }
        }
        createIndexNode(root.getTree("/"), DEFAULT_INDEX_HOME, TEST_INDEX_NAME);
        root.commit(DefaultConflictHandler.OURS);
    }

    protected Result executeQuery(String statement) throws ParseException {
        return qe.executeQuery(statement, SQL2, session, Long.MAX_VALUE, 0,
                null, null);
    }
}