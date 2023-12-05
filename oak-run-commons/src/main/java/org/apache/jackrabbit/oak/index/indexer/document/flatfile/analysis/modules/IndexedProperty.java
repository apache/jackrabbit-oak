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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexedProperty {
    private final String name;
    private final ArrayList<String> parents;
    private final String nodeType;

    IndexedProperty(String name, ArrayList<String> parents, String nodeType) {
        this.name = name;
        this.parents = parents;
        this.nodeType = nodeType;
    }

    public static IndexedProperty create(String nodeType, String fullName) {
        String[] list = fullName.split("/");
        String name = list[list.length - 1];
        ArrayList<String> parents = new ArrayList<>(Arrays.asList(list));
        parents.remove(parents.size() - 1);
        return new IndexedProperty(name, parents, nodeType);
    }

    public String toString() {
        return nodeType + ":" + String.join("/", parents) + " " + name;
    }

    public boolean matches(String name, List<String> pathElements) {
        if (!name.equals(this.name)) {
            return false;
        }
        if (pathElements.size() < parents.size()) {
            return false;
        }
        for (int i = 0; i < parents.size(); i++) {
            String pr = parents.get(i);
            String pe = pathElements.get(pathElements.size() - parents.size() + i);
            if (!pr.equals(pe)) {
                return false;
            }
        }
        return true;
    }

    public String getPropertyName() {
        return name;
    }
}
