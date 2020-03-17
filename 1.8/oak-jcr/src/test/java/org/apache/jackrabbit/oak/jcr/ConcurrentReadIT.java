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

package org.apache.jackrabbit.oak.jcr;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

/**
 * Test cases asserting concurrent session access does not
 * corrupt Oak internal data structures.
 */
public class ConcurrentReadIT extends AbstractRepositoryTest {
    public ConcurrentReadIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void concurrentNodeIteration()
            throws RepositoryException, InterruptedException, ExecutionException {
        final Session session = createAdminSession();
        try {
            final Node testRoot = session.getRootNode().addNode("test-root");
            for (int k = 0; k < 50; k++) {
                testRoot.addNode("n" + k);
            }
            session.save();

            ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
                    Executors.newCachedThreadPool());

            List<ListenableFuture<?>> futures = Lists.newArrayList();
            for (int k = 0; k < 20; k ++) {
                futures.add(executorService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        for (int k = 0; k < 10000; k++) {
                            session.refresh(false);
                            NodeIterator children = testRoot.getNodes();
                            children.hasNext();
                        }
                        return null;
                    }
                }));
            }

            // Throws ExecutionException if any of the submitted task failed
            Futures.allAsList(futures).get();
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        } finally {
            session.logout();
        }
    }

    @Test
    public void concurrentPropertyIteration()
            throws RepositoryException, InterruptedException, ExecutionException {
        final Session session = createAdminSession();
        try {
            final Node testRoot = session.getRootNode().addNode("test-root");
            for (int k = 0; k < 50; k++) {
                testRoot.setProperty("p" + k, k);
            }
            session.save();

            ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
                    Executors.newCachedThreadPool());

            List<ListenableFuture<?>> futures = Lists.newArrayList();
            for (int k = 0; k < 20; k ++) {
                futures.add(executorService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        for (int k = 0; k < 100000; k++) {
                            session.refresh(false);
                            PropertyIterator properties = testRoot.getProperties();
                            properties.hasNext();
                        }
                        return null;
                    }
                }));
            }

            // Throws ExecutionException if any of the submitted task failed
            Futures.allAsList(futures).get();
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        } finally {
            session.logout();
        }
    }

}
