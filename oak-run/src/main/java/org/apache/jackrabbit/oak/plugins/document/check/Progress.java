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

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.plugins.document.check.DocumentProcessor.nowAsISO8601;

/**
 * <code>Progress</code>...
 */
public class Progress implements DocumentProcessor {

    private long numDocuments = 0;

    @Override
    public void processDocument(@NotNull NodeDocument document,
                                @NotNull BlockingQueue<Result> results)
            throws InterruptedException {
        if (++numDocuments % 10000 == 0) {
            results.put(newProgressResult(numDocuments, document.getPath()));
        }
    }

    protected Result newProgressResult(long numDocs, Path path) {
        return new ProgressResult(numDocs, path);
    }

    static class ProgressResult implements Result {

        protected final String msg;

        public ProgressResult(long numDocs, Path path) {
            this.msg = "Checked " + numDocs + " documents so far - " + path;
        }

        @Override
        public String toJson() {
            JsopBuilder json = new JsopBuilder();
            json.object();
            json.key("time").value(nowAsISO8601());
            json.key("info").value(msg);
            json.endObject();
            return json.toString();
        }
    }
}
