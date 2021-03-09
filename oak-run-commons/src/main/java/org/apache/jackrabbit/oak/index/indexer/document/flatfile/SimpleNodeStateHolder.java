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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.List;

import org.apache.jackrabbit.oak.commons.StringUtils;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter.getPath;

class SimpleNodeStateHolder implements NodeStateHolder{
    private final String line;
    private final List<String> pathElements;

    public SimpleNodeStateHolder(String line) {
        this.pathElements = copyOf(elements(getPath(line)));
        this.line = line;
    }

    @Override
    public List<String> getPathElements() {
        return pathElements;
    }

    /**
     * Line here contains the path also
     */
    @Override
    public String getLine() {
        return line;
    }

    @Override
    public int getMemorySize() {
        int memoryUsed = 0;
        for (String e : pathElements) {
            memoryUsed += StringUtils.estimateMemoryUsage(e);
        }
        memoryUsed += StringUtils.estimateMemoryUsage(line);
        return memoryUsed;
    }
}
