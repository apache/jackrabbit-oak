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
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Microbenchmark isolating the per-commit overhead of having the audit
 * pipeline wired but no audit capture site firing. Each iteration does
 * {@code commitsPerIteration} small {@code setProperty} + {@code save}
 * cycles on a fixed leaf node. No {@code UserManager} traffic, so no
 * {@code AuditDispatch.record} calls happen — the only audit-attributable
 * work per commit is:
 * <ul>
 *   <li>{@code AuditBufferLifecycle.onRefresh(sessionId)} on each
 *   {@code root.refresh()} pre-commit;</li>
 *   <li>one extra observer in the commit-dispatch chain
 *   ({@code AuditDrainObserver}) that short-circuits on empty buffer.</li>
 * </ul>
 *
 * Delta = (Oak-MemoryNS-Audit median) − (Oak-MemoryNS median), divided
 * by {@code commitsPerIteration}, is the per-commit audit-pipeline-on
 * overhead when no event is captured.
 *
 * <p>Tunable via {@code -DcommitsPerIteration=N} (default 5000).
 */
public class AuditEmptyCommitOverheadTest extends AbstractTest<Object> {

    private static final int COMMITS_PER_ITERATION =
            Integer.getInteger("commitsPerIteration", 5000);

    private Session session;
    private Node leaf;
    private int iteration;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
        Node root = session.getRootNode().addNode(
                "AuditEmptyCommitOverhead-" + TEST_ID, "nt:unstructured");
        leaf = root.addNode("leaf", "nt:unstructured");
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        // distinct property names per iteration so we don't fight the
        // diff machinery's "same value, no commit" optimisation
        String prefix = "p" + iteration++ + "_";
        for (int i = 0; i < COMMITS_PER_ITERATION; i++) {
            leaf.setProperty(prefix + i, i);
            session.save();
        }
    }

    @Override
    public void afterSuite() throws RepositoryException {
        leaf.getParent().remove();
        session.save();
        logout(session);
    }
}
