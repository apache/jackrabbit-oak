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
package org.apache.jackrabbit.oak.plugins.document.check;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * <code>OrphanedNodeCheck</code>...
 */
public class OrphanedNodeCheck implements DocumentProcessor, Closeable {

    private final DocumentNodeStore ns;

    private final RevisionVector headRevision;

    private final ExecutorService executorService;

    public OrphanedNodeCheck(DocumentNodeStore ns,
                             RevisionVector headRevision,
                             int numThread) {
        this.ns = ns;
        this.headRevision = headRevision;
        this.executorService = new ThreadPoolExecutor(
                numThread, numThread, 1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public void processDocument(@NotNull NodeDocument document,
                                @NotNull BlockingQueue<Result> results) {
        if (!document.isSplitDocument()) {
            executorService.submit(new CheckDocument(ns, headRevision, document, results));
        }
    }

    @Override
    public void end(@NotNull BlockingQueue<Result> results)
            throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
            String msg = "Checks still not finished after the last one has been submitted 5 minutes ago";
            results.put(() -> {
                JsopBuilder json = new JsopBuilder();
                json.object();
                json.key("time").value(nowAsISO8601());
                json.key("info").value(msg);
                json.endObject();
                return json.toString();
            });
        }
    }

    @Override
    public void close() throws IOException {
        new ExecutorCloser(executorService, 5, TimeUnit.MINUTES).close();
    }

    private static final class CheckDocument implements Callable<Void> {

        private final DocumentNodeStore ns;

        private final RevisionVector headRevision;

        private final NodeDocument doc;

        private final BlockingQueue<Result> results;

        CheckDocument(DocumentNodeStore ns,
                      RevisionVector headRevision,
                      NodeDocument doc,
                      BlockingQueue<Result> results) {
            this.ns = ns;
            this.headRevision = headRevision;
            this.doc = doc;
            this.results = results;
        }

        @Override
        public Void call() throws Exception {
            DocumentNodeState state = doc.getNodeAtRevision(ns, headRevision, null);
            if (state != null) {
                Path path = doc.getPath();
                Path missing = assertAncestorsExist(path.getParent());
                if (missing != null) {
                    results.put(new OrphanedNode(path, missing, state.getLastRevision()));
                }
            }
            return null;
        }

        private Path assertAncestorsExist(Path path) {
            if (path == null) {
                return null;
            }
            NodeState state = ns.getRoot();
            Path p = Path.ROOT;
            for (String name : path.elements()) {
                p = new Path(p, name);
                state = state.getChildNode(name);
                if (!state.exists()) {
                    return p;
                }
            }
            return null;
        }
    }

    private static final class OrphanedNode implements Result {

        final Path orphaned;

        final Path missing;

        final RevisionVector revision;

        public OrphanedNode(Path orphaned, Path missing, RevisionVector revision) {
            this.orphaned = orphaned;
            this.missing = missing;
            this.revision = revision;
        }


        @Override
        public String toJson() {
            JsopBuilder json = new JsopBuilder();
            json.object();
            json.key("orphaned").value(orphaned.toString());
            json.key("revision").value(revision.toString());
            json.key("missing").value(missing.toString());
            json.endObject();
            return json.toString();
        }
    }
}
