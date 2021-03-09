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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

class StateInBytesHolder implements NodeStateHolder {
    private final List<String> pathElements;
    private final byte[] content;

    public StateInBytesHolder(String path, String line) {
        this.pathElements = copyOf(elements(path));
        this.content = line.getBytes(UTF_8);
    }

    @Override
    public List<String> getPathElements() {
        return pathElements;
    }

    /**
     * Line here does not include the path
     */
    @Override
    public String getLine() {
        return new String(content, UTF_8);
    }

    @Override
    public int getMemorySize() {
        int memoryUsed = 0;
        for (String e : pathElements) {
            memoryUsed += StringUtils.estimateMemoryUsage(e);
        }
        memoryUsed += content.length;
        return memoryUsed;
    }
}
