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
package org.apache.jackrabbit.oak.jcr.query;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;

/**
 * Tests the Lucene index using multiple threads.
 * 
 * See https://issues.apache.org/jira/browse/OAK-837
 */
public class MultiSessionQueryTest {

    final static int THREAD_COUNT = 3;

    private Repository repository = null;

    @Before
    public void before() {
        Jcr jcr = new Jcr();

        // lucene specific
        jcr.with(new LuceneInitializerHelper("lucene").async());
        LuceneIndexProvider provider = new LuceneIndexProvider();
        jcr.with((QueryIndexProvider) provider);
        jcr.with((Observer) provider);
        jcr.with(new LuceneIndexEditorProvider());

        repository = jcr.createRepository();
    }

    @After
    public void after() {
        shutdown(repository);
    }

    protected Session createAdminSession() throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin"
                .toCharArray()));
    }

    @Test
    public void testConcurrent() throws Exception {

        final Exception[] ex = new Exception[1];
        Thread[] threads = new Thread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            final Session s = createAdminSession();
            final String node = "node" + i;
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        doTest(s, node);
                    } catch (Exception e) {
                        ex[0] = e;
                    }
                }
            };
            threads[i] = t;
        }
        for (Thread t : threads) {
            t.start();
        }
        Thread.sleep(100);
        for (Thread t : threads) {
            t.join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
    }

    void doTest(Session s, String node) throws Exception {
        Node root = s.getRootNode();
        if (!root.hasNode(node)) {
            root.addNode(node);
            s.save();
        }
        for (int i = 0; i < 10; i++) {
            // Thread.sleep(100);
            // System.out.println("session " + node + " work");
        }
        s.logout();
    }

}
