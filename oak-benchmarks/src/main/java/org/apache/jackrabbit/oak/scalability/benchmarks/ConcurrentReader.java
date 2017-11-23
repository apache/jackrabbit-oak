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
package org.apache.jackrabbit.oak.scalability.benchmarks;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Reads random paths concurrently with multiple readers/writers configured with {@link #WRITERS} and {@link #READERS}.
 *
 * <p>
 * The following system JVM properties can be defined to configure the benchmark behavior.
 * <ul>
 * <li>
 *     <code>concurrentWriters</code> - Controls the number of concurrent background threads for writing nodes.
 *     Defaults to 0.
 * </li>
 * <li>
 *     <code>concurrentReaders</code> - Controls the number of concurrent background threads for reading nodes.
 *     Defaults to 0.
 * </li>
 * <li>
 *     <code>assets</code> - Controls the number of nodes to read/write in the background threads.
 *     Defaults to 100.
 * </li>
 * </ul>
 *
 */
public class ConcurrentReader extends ScalabilityBenchmark {
    protected static final Logger LOG = LoggerFactory.getLogger(ConcurrentReader.class);
    private static final Random rand = new Random();
    private static final int WRITERS = Integer.getInteger("concurrentReaders", 0);
    private static final int READERS = Integer.getInteger("concurrentWriters", 0);
    private static final int MAX_ASSETS = Integer.getInteger("assets", 100);
    private static final String ROOT_NODE_NAME =
        ConcurrentReader.class.getSimpleName() + UUID.randomUUID();

    private boolean running;
    private List<Thread> jobs = Lists.newArrayList();

    @Override
    public void beforeExecute(Repository repository, Credentials credentials, ExecutionContext context) throws Exception {
        Session session = repository.login(credentials);
        JcrUtils.getOrAddNode(session.getRootNode(), ROOT_NODE_NAME);
        session.save();
        session.logout();

        for(int idx = 0; idx < WRITERS; idx++) {
            try {
                Thread thread = createJob(
                    new Writer("concurrentWriter-" + UUID.randomUUID() + idx, MAX_ASSETS,
                        repository.login(credentials), context));
                jobs.add(thread);
                thread.start();
            } catch (Exception e) {
                LOG.error("error creating background writer", e);
            }
        }

        for(int idx = 0; idx < READERS; idx++) {
            try {
                Thread thread = createJob(
                    new Reader("concurrentReader-" + UUID.randomUUID() + idx, MAX_ASSETS,
                        repository.login(credentials), context));
                jobs.add(thread);
                thread.start();
            } catch (Exception e) {
                LOG.error("error creating background reader", e);
            }
        }
        running = true;
    }

    private Thread createJob(final Job job) throws RepositoryException {
        Thread thread = new Thread(job.id) {
            @Override public void run() {
                while (running) {
                    job.process();
                }
            }
        };
        thread.setDaemon(true);
        return thread;
    }

    @Override
    public void afterExecute(Repository repository, Credentials credentials, ExecutionContext context) {
        running = false;
        for (Thread thread : jobs) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                LOG.error("Error stopping thread", e);
            }
        }
        jobs.clear();
    }

    @Override
    public void execute(Repository repository, Credentials credentials, ExecutionContext context)
        throws Exception {
        Reader reader = new Reader(this.getClass().getSimpleName() + UUID.randomUUID(),
            100, repository.login(credentials), context);
        reader.process();
    }

    abstract class Job {
        final Node parent;
        final Session session;
        final String id;
        final int maxAssets;
        final List<String> readPaths;
        final Random rand;

        Job(String id, int maxAssets, Session session, ExecutionContext context) throws RepositoryException {
            this.id = id;
            this.maxAssets = maxAssets;
            this.session = session;
            this.parent = session
                .getRootNode()
                .getNode(ROOT_NODE_NAME)
                .addNode(id);
            readPaths =
                (List<String>) context.getMap().get(ScalabilityAbstractSuite.CTX_SEARCH_PATHS_PROP);
            rand = new Random();
            session.save();
        }

        public abstract void process();
    }


    /**
     * Simple Reader job
     */
    class Reader extends Job {
        Reader(String id, int maxAssets, Session session, ExecutionContext context) throws RepositoryException {
            super(id, maxAssets, session, context);
        }

        @Override
        public void process() {
            try {
                int count = 1;
                int readPathSize = readPaths.size();
                while (count <= maxAssets) {
                    session.refresh(false);
                    Node node =
                        JcrUtils.getNodeIfExists(readPaths.get(rand.nextInt(readPathSize)), session);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(node.getPath());
                    }
                    count++;
                }
            } catch (Exception e) {
                LOG.error("Exception in reading", e);
            }
        }
    }


    /**
     * Simple Writer job
     */
    class Writer extends Job {
        Writer(String id, int maxAssets, Session session, ExecutionContext context) throws RepositoryException {
            super(id, maxAssets, session, context);
        }

        @Override
        public void process() {
            try {
                int count = 1;
                while (count <= maxAssets) {
                    session.refresh(false);
                    Node node =
                        JcrUtils.getOrAddNode(parent, "Node" + count, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                    node.setProperty("prop1", "val1");
                    node.setProperty("prop2", "val2");
                    session.save();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(node.getPath());
                    }
                    count++;
                }
            } catch (Exception e) {
                LOG.error("Exception in write", e);
            }
        }
    }

}
