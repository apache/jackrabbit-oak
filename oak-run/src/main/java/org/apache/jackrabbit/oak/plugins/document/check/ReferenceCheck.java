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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Checks if {@code jcr:baseVersion} reference properties resolve to a node.
 */
public class ReferenceCheck extends AsyncNodeStateProcessor {

    private final String propertyName;

    ReferenceCheck(String propertyName,
                   DocumentNodeStore ns,
                   RevisionVector headRevision,
                   ExecutorService executorService) {
        super(ns, headRevision, executorService);
        this.propertyName = propertyName;
    }

    @Override
    protected Optional<Result> runTask(@NotNull Path path,
                                       @Nullable NodeState state) {
        if (state == null) {
            return Optional.empty();
        }
        PropertyState prop = state.getProperty(propertyName);
        Result result = null;
        if (prop != null) {
            String ref = prop.getValue(Type.REFERENCE);
            AtomicReference<String> resolvedPath = new AtomicReference<>("");
            NodeState version = getNodeByUUID(ref, resolvedPath);
            if (version == null) {
                result = new BrokenReference(path, ref, resolvedPath.get());
            }
        }
        return Optional.ofNullable(result);
    }

    private final class BrokenReference implements Result {

        private final Path path;

        private final String reference;

        private final String resolvedPath;

        public BrokenReference(@NotNull Path path,
                               @NotNull String reference,
                               @NotNull String resolvedPath) {
            this.path = path;
            this.reference = reference;
            this.resolvedPath = resolvedPath;
        }

        @Override
        public String toJson() {
            JsopBuilder json = new JsopBuilder();
            json.object();
            json.key("type").value("reference");
            json.key("path").value(new Path(path, propertyName).toString());
            json.key("uuid").value(reference);
            json.key("resolved").value(resolvedPath);
            json.key("revision").value(headRevision.toString());
            json.endObject();
            return json.toString();
        }
    }
}
