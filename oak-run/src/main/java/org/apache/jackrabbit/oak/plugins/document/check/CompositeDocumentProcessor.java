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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.jetbrains.annotations.NotNull;

/**
 * <code>CompositeDocumentProcessor</code>...
 */
public class CompositeDocumentProcessor implements DocumentProcessor {

    private final List<DocumentProcessor> processors;

    private CompositeDocumentProcessor(List<DocumentProcessor> processors) {
        this.processors = processors;
    }

    public static DocumentProcessor compose(Iterable<DocumentProcessor> processors) {
        List<DocumentProcessor> processorList = new ArrayList<>();
        processors.forEach(processorList::add);
        if (processorList.size() == 1) {
            return processorList.get(0);
        } else {
            return new CompositeDocumentProcessor(processorList);
        }
    }

    @Override
    public void processDocument(@NotNull NodeDocument document,
                                @NotNull BlockingQueue<Result> results)
            throws InterruptedException {
        for (DocumentProcessor p : processors) {
            p.processDocument(document, results);
        }
    }

    @Override
    public void end(@NotNull BlockingQueue<Result> results)
            throws InterruptedException {
        for (DocumentProcessor p : processors) {
            p.end(results);
        }
    }
}
