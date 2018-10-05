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

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

public class ManyNodes extends Benchmark {

    private final boolean verbose;

    ManyNodes(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        for (RepositoryFixture fixture : fixtures) {
            System.out.println("ManyNodes test: " + fixture);
            if (fixture.isAvailable(1)) {
                try {
                    Repository[] cluster = fixture.setUpCluster(1);
                    try {
                        Session session = cluster[0].login(new SimpleCredentials(
                                "admin", "admin".toCharArray()));
                        try {
                            addManyNodes(session);
                        } finally {
                            session.logout();
                        }
                    } finally {
                        fixture.tearDownCluster();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("- not available, skipping");
            }
        }
    }


    private void addManyNodes(Session session) throws RepositoryException {
        Node root = session.getRootNode().addNode("testRoot");
        session.save();
        int total = 0;
        long time = System.currentTimeMillis();
        for (int k = 0; k < 1000; k++) {
            Node nk = root.addNode("test" + k, "nt:folder");
            for (int j = 0; j < 100; j++) {
                Node nj = nk.addNode("test" + j, "nt:folder");
                for (int i = 0; i < 100; i++) {
                    nj.addNode("child" + i, "nt:folder");
                    total++;
                }
                session.save();
                if (total % 10000 == 0) {
                    long now = System.currentTimeMillis();
                    if (verbose) {
                        System.out.println(total + " nodes in " + (now - time) + " ms");
                    }
                    time = now;
                }
            }
        }
    }

}
