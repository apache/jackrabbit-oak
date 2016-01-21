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

import javax.jcr.LoginException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

public class ReplicaCrashResilienceTest extends Benchmark {

    private static final String LEVEL2POINTER = "level2pointer";
    private static final String LEVEL1POINTER = "level1pointer";
    private static final String WRITER_INFOS = "writerInfos";
    private static final String REPLICA_CRASH_TEST = "replicaCrashTest-"+System.currentTimeMillis();

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        for (RepositoryFixture fixture : fixtures) {
            if (fixture.isAvailable(1)) {
                System.out.format("%s: ReplicaCrashResilienceTest%n", fixture);
                try {
                    Repository[] cluster = fixture.setUpCluster(1);
                    try {
                        run(cluster[0]);
                    } finally {
                        fixture.tearDownCluster();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    private void run(final Repository repository) throws Exception {
        System.out.println("Setup...");
        try {
            Session session = repository.login(
                    new SimpleCredentials("admin", "admin".toCharArray()));
            final Node rootNode = session.getRootNode();
            if (rootNode.hasNode(REPLICA_CRASH_TEST)) {
                // then cleanup first
                rootNode.getNode(REPLICA_CRASH_TEST).remove();
                session.save();
            }
            session.refresh(false);
            final Node replicaCrashTestNode = rootNode.addNode(REPLICA_CRASH_TEST);
            replicaCrashTestNode.addNode(WRITER_INFOS);
            session.save();
        } catch (RepositoryException e1) {
            e1.printStackTrace();
            System.exit(1);
        }
        // this runnable will go ahead and start writing nodes to the repo
        // the structure is:
        // /replicaCrashTest/writerInfos
        //                           - level 1 pointer
        //                           - level 2 pointer
        // /replicaCrashTest/[1-n]/[1-1000]
        Runnable writer = new Runnable() {

            @Override
            public void run() {
                int level1Pointer = 1;
                int level2Pointer = 1;
                Session session = null;
                try {
                    session = repository.login(
                            new SimpleCredentials("admin", "admin".toCharArray()));
                } catch (Exception e1) {
                    e1.printStackTrace();
                    System.exit(1);
                }
                System.out.println("Writer: Test start.");
                while(true) {
                    try{
                        final String level1 = String.valueOf(level1Pointer);
                        final String level2 = String.valueOf(level2Pointer);

                        final Node rootNode = session.getRootNode();
                        final Node replicaCrashTestNode = rootNode.getNode(REPLICA_CRASH_TEST);
                        final Node writerInfosNode = replicaCrashTestNode.getNode(WRITER_INFOS);

                        Node level1Node;
                        if (replicaCrashTestNode.hasNode(level1)) {
                            level1Node = replicaCrashTestNode.getNode(level1);
                        } else {
                            level1Node = replicaCrashTestNode.addNode(level1);
                            System.out.println("Writer: Created level1 node: "+level1Node);
                        }
                        Node level2Node = level1Node.addNode(level2);
                        System.out.println("Writer: Created level2 node: "+level2Node);
                        writerInfosNode.setProperty(LEVEL1POINTER, level1Pointer);
                        writerInfosNode.setProperty(LEVEL2POINTER, level2Pointer);
                        session.save();
                    } catch(com.google.common.util.concurrent.UncheckedExecutionException e) {
                        System.out.println("Got an UncheckedException (levels: "+level1Pointer+"/"+level2Pointer+") from the google cache probably: "+e);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e2) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                        continue;
                    } catch (Throwable e) { // yes, one should not catch Throwable - but this is a test only
                        e.printStackTrace(System.out);
                        System.exit(1);
                    }
                    if (++level2Pointer>1000) {
                        level2Pointer = 1;
                        level1Pointer++;
                    }
                }
            }

        };
        Thread th1 = new Thread(writer);
        System.out.println("Launching writer...");
        th1.start();

        Runnable reader = new Runnable() {

            @Override
            public void run() {
                long level1Pointer = 1;
                long level2Pointer = 1;
                Session session = null;
                try {
                    session = repository.login(
                            new SimpleCredentials("admin", "admin".toCharArray()));

                    Node rootNode = session.getRootNode();
                    Node replicaCrashTestNode = rootNode.getNode(REPLICA_CRASH_TEST);
                    Node writerInfos;

                    while(true) {
                        try{
                            final String level1 = String.valueOf(level1Pointer);
                            final String level2 = String.valueOf(level2Pointer);
                            session.refresh(false);
                            writerInfos = replicaCrashTestNode.getNode(WRITER_INFOS);
                            long writerLevel1Pointer = writerInfos.getProperty(LEVEL1POINTER).getLong();
                            long writerLevel2Pointer = writerInfos.getProperty(LEVEL2POINTER).getLong();

                            long writerPointer = writerLevel1Pointer * 1000 + writerLevel2Pointer;
                            long myPointer = level1Pointer * 1000 + level2Pointer;
                            long diff = writerPointer - myPointer;
                            if (diff<100) {
                                System.out.println("Reader: Closer than 100, waiting...level1="+level1+", level2="+level2);
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                    System.exit(1);
                                }
                                continue;
                            }


                            rootNode = session.getRootNode();
                            replicaCrashTestNode = rootNode.getNode(REPLICA_CRASH_TEST);

                            Node level1Node = replicaCrashTestNode.getNode(level1);
                            if (!level1Node.hasNode(level2)) {
                                System.err.println("Reader: NOT FOUND: level1="+level1+", level2="+level2);
                                Thread.sleep(500);
                                session.refresh(false);
                                System.err.println("Reader: Reverifying once...");
                                rootNode = session.getRootNode();
                                replicaCrashTestNode = rootNode.getNode(REPLICA_CRASH_TEST);
                                level1Node = replicaCrashTestNode.getNode(level1);
                                final boolean hasNode = level1Node.hasNode(level2);
                                if (hasNode) {
                                    System.err.println("Reader: yup, exists: "+hasNode+", level1="+level1+", level2="+level2);
                                } else {
                                    System.err.println("Reader: not found: level1="+level1+", level2="+level2);

                                }
                            } else {
                                // read it
                                Node level2Node = level1Node.getNode(level2);
                                System.out.println("Reader: verified level1="+level1+", level2="+level2);
                            }

                        } catch(com.google.common.util.concurrent.UncheckedExecutionException e) {
                            System.out.println("Got an UncheckedException from the google cache probably: "+e);
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e2) {
                                e.printStackTrace();
                                System.exit(1);
                            }
                            continue;
                        } catch (Throwable e) { // yes, one should not catch Throwable - but this is a test only
                            e.printStackTrace(System.out);
                            System.exit(1);
                        }
                        if (++level2Pointer>1000) {
                            level2Pointer = 1;
                            level1Pointer++;
                        }
                    }

                } catch (RepositoryException e1) {
                    e1.printStackTrace(System.out);
                    System.exit(1);
                }
                System.out.println("Test start.");

            }
        };
        Thread th2 = new Thread(reader);
        Thread.sleep(1000);
        th2.start();

        System.out.println("Waiting for writer to finish...");
        th1.join();
    }
}