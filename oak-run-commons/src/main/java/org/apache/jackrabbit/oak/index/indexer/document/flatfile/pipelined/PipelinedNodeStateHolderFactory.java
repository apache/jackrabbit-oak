/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import java.util.ArrayList;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter.getPath;

final class PipelinedNodeStateHolderFactory {
    private final ArrayList<String> arrayBuilder = new ArrayList<>(16);

    public PipelinedNodeStateHolder create(String line) {
        String path = getPath(line);
        arrayBuilder.clear();
        for (String part : elements(path)) {
            arrayBuilder.add(part);
        }
        String[] pathElements = arrayBuilder.toArray(new String[0]);
        return new PipelinedNodeStateHolder(line, pathElements);
    }

}
