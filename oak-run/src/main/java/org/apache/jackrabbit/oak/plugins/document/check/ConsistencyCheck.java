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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.Consistency;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.jetbrains.annotations.NotNull;

/**
 * <code>ConsistencyCheck</code>...
 */
public class ConsistencyCheck extends AsyncDocumentProcessor {

    private final DocumentNodeStore ns;

    private final DocumentNodeState root;

    public ConsistencyCheck(DocumentNodeStore ns, ExecutorService executorService) {
        super(executorService);
        this.ns = ns;
        this.root = ns.getRoot();
    }

    @Override
    protected Optional<Callable<Void>> createTask(@NotNull NodeDocument document,
                                                  @NotNull BlockingQueue<Result> results) {
        if (document.isSplitDocument()) {
            return Optional.empty();
        } else {
            return Optional.of(new CheckDocument(ns, root, document, results));
        }
    }

    private static final class CheckDocument implements Callable<Void> {

        private final DocumentNodeStore ns;

        private final DocumentNodeState root;

        private final NodeDocument doc;

        private final BlockingQueue<Result> results;

        public CheckDocument(DocumentNodeStore ns,
                             DocumentNodeState root,
                             NodeDocument doc,
                             BlockingQueue<Result> results) {
            this.ns = ns;
            this.root = root;
            this.doc = doc;
            this.results = results;
        }

        @Override
        public Void call() throws Exception {
            Set<Revision> revisions = new HashSet<>();
            new Consistency(root, doc).check(ns, revisions::add);
            for (Revision revision : revisions) {
                results.put(new InconsistentState(doc.getPath(), revision));
            }
            return null;
        }
    }

    private static final class InconsistentState implements Result {

        final Path path;

        final Revision revision;

        public InconsistentState(Path path, Revision revision) {
            this.path = path;
            this.revision = revision;
        }

        @Override
        public String toJson() {
            JsopBuilder json = new JsopBuilder();
            json.object();
            json.key("type").value("inconsistent");
            json.key("path").value(path.toString());
            json.key("revision").value(revision.toString());
            json.endObject();
            return json.toString();
        }
    }
}