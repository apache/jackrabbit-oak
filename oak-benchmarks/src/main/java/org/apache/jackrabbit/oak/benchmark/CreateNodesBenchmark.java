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
package org.apache.jackrabbit.oak.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

/**
 * Creates approximately 100k nodes (breadth first, save every 10 nodes).
 */
public class CreateNodesBenchmark extends Benchmark {

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        for (RepositoryFixture fixture : fixtures) {
            if (fixture.isAvailable(1)) {
                System.out.format("%s: Create nodes benchmark%n", fixture);
                try {
                    Repository[] cluster = fixture.setUpCluster(1);
                    try {
                        run(cluster[0]);
                    } finally {
                        fixture.tearDownCluster();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void run(Repository repository) throws RepositoryException {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        AtomicInteger count = new AtomicInteger();
        long startTime = System.currentTimeMillis();
        Node testRoot = session.getRootNode().addNode("r" + AbstractTest.TEST_ID);
        createNodes(testRoot, 20, 6, count, startTime);
        long duration = System.currentTimeMillis() - startTime;
        System.out.format(
                "Created %d nodes in %d seconds (%.2fms/node)%n",
                count.get(), duration / 1000, (double) duration / count.get());
    }

    private void createNodes(Node n, int nodesPerLevel,
                            int levels, AtomicInteger count, long startTime)
            throws RepositoryException {
        levels--;
        List<Node> nodes = new ArrayList<Node>();
        for (int i = 0; i < nodesPerLevel; i++) {
            nodes.add(n.addNode("folder-" + i, "nt:folder"));
            if (count.incrementAndGet() % 1000 == 0) {
                long duration = System.currentTimeMillis() - startTime;
                System.out.format(
                        "Created %d nodes in %d seconds (%.2fms/node)...%n",
                        count.get(), duration / 1000,
                        (double) duration / count.get());
            }
        }
        n.getSession().save();
        if (levels > 0) {
            for (Node child : nodes) {
                createNodes(child, nodesPerLevel, levels, count, startTime);
            }
        }
    }
}
