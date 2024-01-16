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


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

interface FieldSizeTracker {

    void addField(String name, int size);

    Iterator<Map.Entry<String, Long>> getKnownFields();

    Long getField(String name);


    class NOOP implements FieldSizeTracker {

        @Override
        public void addField(String name, int size) {
        }

        @Override
        public Iterator<Map.Entry<String, Long>> getKnownFields() {
            return Map.<String, Long>of().entrySet().iterator();
        }

        @Override
        public Long getField(String name) {
            return null;
        }
    }

    class HashMapFieldSizeTracker implements FieldSizeTracker {
        private final Map<String, Long> knownFields = new HashMap<>();

        @Override
        public void addField(String name, int size) {
            // If field is not present, add it. Otherwise add the size to the value
            knownFields.merge(name, (long) size, Long::sum);
        }

        @Override
        public Iterator<Map.Entry<String, Long>> getKnownFields() {
            return knownFields.entrySet().iterator();
        }

        @Override
        public Long getField(String name) {
            return knownFields.get(name);
        }
    }
}



