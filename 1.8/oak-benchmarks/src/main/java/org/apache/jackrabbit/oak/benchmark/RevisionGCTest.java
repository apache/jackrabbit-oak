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

import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.base.Stopwatch;

/**
 * A benchmark to run RevisionGC.
 */
public class RevisionGCTest extends Benchmark {

    protected static final float GARBAGE_RATIO = Float.parseFloat(
            System.getProperty("garbageRatio", "0.5"));

    protected static final String NODE_TYPE =
            System.getProperty("nodeType", "nt:unstructured");

    protected static final int SCALE = AbstractTest.getScale(100);

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        for (RepositoryFixture fixture : fixtures) {
            if (fixture.isAvailable(1)) {
                System.out.format("%s: RevisionGC benchmark%n", fixture);
                try {
                    final AtomicReference<Oak> whiteboardRef = new AtomicReference<Oak>();
                    Repository[] cluster;
                    if (fixture instanceof OakRepositoryFixture) {
                        cluster = ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                            @Override
                            public Jcr customize(Oak oak) {
                                whiteboardRef.set(oak);
                                return new Jcr(oak);
                            }
                        });
                    } else {
                        System.err.format("%s: RevisionGC benchmark only runs on Oak%n", fixture);
                        return;
                    }
                    try {
                        run(cluster[0], getNodeStore(whiteboardRef.get()));
                    } finally {
                        fixture.tearDownCluster();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected void run(Repository repository, NodeStore nodeStore)
            throws Exception {
        Session s = createSession(repository);
        Random rand = new Random();
        try {
            System.out.print("Creating garbage ");
            String longPathName = "0123456789";
            int depth = Integer.getInteger("RevisionGCTest.PATHDEPTH", 0);
            Node p = s.getRootNode();
            while (depth > 0) {
                depth -= 1;
                p = p.addNode(longPathName);
            }
            s.save();
            System.out.println("Creating garbage in " + p.getPath() + " (" + p.getPath().length() + " chars)");
            for (int i = 0; i < SCALE; i++) {
                Node n = p.addNode("node-" + i);
                for (int j = 0; j < 1000; j++) {
                    n.addNode("child-" + j, NODE_TYPE);
                }
                s.save();
                if (rand.nextFloat() <= GARBAGE_RATIO) {
                    n.remove();
                    s.save();
                }
                System.out.print(".");
            }
            System.out.println();
            System.out.println("Running RevisionGC");
            Stopwatch sw = Stopwatch.createStarted();
            String result = revisionGC(nodeStore);
            sw.stop();
            System.out.println(result);
            System.out.println("Performed RevisionGC in " + sw);
        } finally {
            s.logout();
        }
    }

    protected static String revisionGC(NodeStore nodeStore) throws Exception {
        if (nodeStore instanceof DocumentNodeStore) {
            return ((DocumentNodeStore) nodeStore).getVersionGarbageCollector()
                    .gc(0, TimeUnit.SECONDS).toString();

        } 
        throw new IllegalArgumentException("Unknown node store: "
                + nodeStore.getClass().getName());
    }

    protected static NodeStore getNodeStore(Oak oak) throws Exception {
        Field f = Oak.class.getDeclaredField("store");
        f.setAccessible(true);
        return (NodeStore) f.get(oak);
    }

    protected static Session createSession(Repository repository)
            throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

}