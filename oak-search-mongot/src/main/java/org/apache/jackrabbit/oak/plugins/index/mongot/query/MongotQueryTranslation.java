/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.List;

import org.bson.Document;

public final class MongotQueryTranslation {

    private final Document searchOperator;
    private final List<Document> pipeline;
    private final String reason;

    private MongotQueryTranslation(Document searchOperator, List<Document> pipeline, String reason) {
        this.searchOperator = searchOperator;
        this.pipeline = pipeline;
        this.reason = reason;
    }

    public static MongotQueryTranslation supported(Document searchOperator) {
        return new MongotQueryTranslation(searchOperator, List.of(), null);
    }

    public static MongotQueryTranslation supported(Document searchOperator, List<Document> pipeline) {
        return new MongotQueryTranslation(searchOperator, List.copyOf(pipeline), null);
    }

    public static MongotQueryTranslation unsupported(String reason) {
        return new MongotQueryTranslation(null, List.of(), reason);
    }

    public boolean isSupported() {
        return reason == null;
    }

    public Document searchOperator() {
        return searchOperator;
    }

    public List<Document> pipeline() {
        return pipeline;
    }

    public String reason() {
        return reason;
    }
}
