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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;

import java.util.Iterator;

final class NodeStateHolder {

    private final String line;
    private final String[] pathElements;

    public NodeStateHolder(String line) {
        this.line = line;
        String path = NodeStateEntryWriter.getPath(line);
        int depth = PathUtils.getDepth(path);
        this.pathElements = new String[depth];
        Iterator<String> iter = PathUtils.elements(path).iterator();
        for (int i = 0; i < pathElements.length; i++) {
            pathElements[i] = iter.next();
        }
    }

    public String[] getPathElements() {
        return pathElements;
    }

    /**
     * Line here contains the path also
     */
    public String getLine() {
        return line;
    }
}
