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
package org.apache.jackrabbit.oak.plugins.lucene;

import java.util.Arrays;
import java.util.List;

/**
 * LuceneIndexInfo contains information about a lucene index
 * 
 */
public class LuceneIndexInfo {

    /**
     * the index name
     */
    private final String name;

    /**
     * the index path, broken into path segments
     */
    private final String[] path;

    public LuceneIndexInfo(String name, String[] path) {
        this.name = name;
        this.path = path;
    }

    public LuceneIndexInfo(String name, List<String> path) {
        this(name, path.toArray(new String[path.size()]));
    }

    public String getName() {
        return name;
    }

    public String[] getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "IndexInfo [name=" + name + ", path=" + Arrays.toString(path)
                + "]";
    }

}
