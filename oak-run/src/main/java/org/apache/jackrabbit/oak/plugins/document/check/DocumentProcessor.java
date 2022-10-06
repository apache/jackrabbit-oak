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

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;

/**
 * Defines an interface to process {@link NodeDocument}s.
 */
public interface DocumentProcessor {

    /**
     * Process the given document and publish the result to the {@code results}
     * queue. An implementation is not required to publish a result for each
     * processed document.
     *
     * @param document the document to process.
     * @param results the queue to publish results if necessary.
     * @throws InterruptedException if publishing a result is interrupted.
     */
    void processDocument(@NotNull NodeDocument document,
                         @NotNull BlockingQueue<Result> results)
            throws InterruptedException;

    /**
     * This method is called after the last document has been processed. An
     * implementation may wish to publish aggregated results at the end.
     *
     * @param results the queue to publish results if necessary.
     * @throws InterruptedException if publishing a result is interrupted.
     */
    default void end(@NotNull BlockingQueue<Result> results)
            throws InterruptedException {
    }

    /**
     * @return the current time as an ISO-8601 formatted string.
     */
    static String nowAsISO8601() {
        return ISO8601.format(System.currentTimeMillis());
    }
}
