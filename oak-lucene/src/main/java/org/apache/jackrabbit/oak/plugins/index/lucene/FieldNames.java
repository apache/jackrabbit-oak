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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Defines field names that are used internally to store :path, etc in the
 * search index.
 */
public final class FieldNames {

    /**
     * Private constructor.
     */
    private FieldNames() {
    }

    /**
     * Name of the field that contains the {@value} property of the node.
     */
    public static final String PATH = ":path";

    /**
     * Used to select only the PATH field from the lucene documents
     */
    public static final Set<String> PATH_SELECTOR = new HashSet<String>(
            Arrays.asList(PATH));

}
