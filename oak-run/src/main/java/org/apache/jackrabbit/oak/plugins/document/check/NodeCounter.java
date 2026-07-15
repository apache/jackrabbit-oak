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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Count documents and nodes that exist.
 */
public class NodeCounter extends AsyncNodeStateProcessor {

    private final AtomicLong numDocuments = new AtomicLong();

    private final AtomicLong numNodes = new AtomicLong();

    public NodeCounter(DocumentNodeStore ns,
                       RevisionVector headRevision,
                       ExecutorService executorService) {
        super(ns, headRevision, executorService);
    }

    @Override
    protected void runTask(@NotNull Path path,
                           @Nullable NodeState state,
                           @NotNull Consumer<Result> resultConsumer) {
        numDocuments.incrementAndGet();
        if (state != null) {
            numNodes.incrementAndGet();
        }
    }

    @Override
    public void end(@NotNull BlockingQueue<Result> results)
            throws InterruptedException {
        JsopBuilder json = new JsopBuilder();
        json.object();
        json.key("type").value("counter");
        json.key("documents").value(numDocuments.get());
        json.key("nodes").value(numNodes.get());
        json.key("exist").value(getPercentageExist());
        json.endObject();
        results.put(json::toString);
    }

    private String getPercentageExist() {
        float p = 0.0f;
        if (numDocuments.get() != 0) {
            p = 100f * numNodes.get() / numDocuments.get();
        }
        return String.format("%.2f%%", p);
    }
}
