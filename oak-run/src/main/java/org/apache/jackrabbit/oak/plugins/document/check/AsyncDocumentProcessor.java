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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for {@link DocumentProcessor} implementations that create tasks
 * executed by an executor service.
 */
public abstract class AsyncDocumentProcessor implements DocumentProcessor {

    private final ExecutorService executorService;

    protected AsyncDocumentProcessor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public final void processDocument(@NotNull NodeDocument document,
                                      @NotNull BlockingQueue<Result> results) {
        Callable<Void> task = createTask(document, results);
        if (task != null) {
            executorService.submit(task);
        }
    }

    protected abstract @Nullable Callable<Void> createTask(@NotNull NodeDocument document,
                                                           @NotNull BlockingQueue<Result> results);
}
