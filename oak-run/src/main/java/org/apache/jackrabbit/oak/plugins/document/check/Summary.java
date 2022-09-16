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

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.jetbrains.annotations.NotNull;

/**
 * <code>Summary</code>...
 */
public class Summary implements DocumentProcessor {

    private final int numThreads;

    private int numDocuments = 0;

    private final Stopwatch sw = Stopwatch.createStarted();

    public Summary(int numThreads) {
        this.numThreads = numThreads;
    }

    @Override
    public void processDocument(@NotNull NodeDocument document,
                                @NotNull BlockingQueue<Result> results) {
        numDocuments++;
    }

    @Override
    public void end(@NotNull BlockingQueue<Result> results)
            throws InterruptedException {
        String summary = "Checked " + numDocuments + " documents in " + sw +
                ". Number of threads used: " + numThreads;
        JsopBuilder json = new JsopBuilder();
        json.object();
        json.key("summary").value(summary);
        json.endObject();
        results.put(json::toString);
    }
}
