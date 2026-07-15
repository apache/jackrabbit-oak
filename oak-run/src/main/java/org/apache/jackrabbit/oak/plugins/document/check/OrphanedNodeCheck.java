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

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

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
public class OrphanedNodeCheck extends AsyncDocumentProcessor {

    private final DocumentNodeStore ns;

    private final RevisionVector headRevision;

    public OrphanedNodeCheck(DocumentNodeStore ns,
                             RevisionVector headRevision,
                             ExecutorService executorService) {
        super(executorService);
        this.ns = ns;
        this.headRevision = headRevision;
    }

    @Override
    protected Optional<Callable<Void>> createTask(@NotNull NodeDocument document,
                                                  @NotNull BlockingQueue<Result> results) {
        if (document.isSplitDocument()) {
            return Optional.empty();
        } else {
            return Optional.of(new CheckDocument(ns, headRevision, document, results));
        }
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
            json.key("type").value("orphan");
            json.key("path").value(orphaned.toString());
            json.key("missing").value(missing.toString());
            json.key("revision").value(revision.toString());
            json.endObject();
            return json.toString();
        }
    }
}
