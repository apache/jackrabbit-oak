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
package org.apache.jackrabbit.mongomk.prototype;

import java.util.Map;

/**
 * Represents a node held in memory (in the cache for example).
 */
public class Node {

    final String path;
    final String rev;
    final Map<String, String> properties = Utils.newMap();
    
    Node(String path, String rev) {
        this.path = path;
        this.rev = rev;
    }
    
    void setProperty(String propertyName, String value) {
        properties.put(propertyName, value);
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("path: ").append(path).append('\n');
        buff.append("rev: ").append(rev).append('\n');
        buff.append(properties);
        buff.append('\n');
        return buff.toString();
    }
    
}
