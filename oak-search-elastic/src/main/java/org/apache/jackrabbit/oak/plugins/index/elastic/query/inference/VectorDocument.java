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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import java.util.List;
import java.util.Map;

public class VectorDocument {

    public static final String ID = "id";
    public static final String VECTOR = "vector";
    public static final String METADATA = "metadata";

    public final String id;

    public final List<Float> vector;

    public final Map<String, Object> metadata;

    public VectorDocument() {
        this.id = null;
        this.vector = null;
        this.metadata = null;
    }

    public VectorDocument(String id, List<Float> vector, Map<String, Object> metadata) {
        this.id = id;
        this.vector = vector;
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "VectorDocument{" +
                "id='" + id + '\'' +
                ", vector=" + vector +
                ", metadata=" + metadata +
                '}';
    }
}