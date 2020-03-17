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
package org.apache.jackrabbit.oak.namepath;

import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * Simple utility class for lazily tracking the current path during
 * a tree traversal that recurses down a subtree.
 */
public class PathTracker {

    private final PathTracker parent;

    private final String name;

    private String path;

    public PathTracker() {
        this.parent = null;
        this.name = null;
        this.path = "/";
    }

    private PathTracker(PathTracker parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null; // initialize lazily
    }

    public PathTracker getChildTracker(String name) {
        return new PathTracker(this, name);
    }

    public String getPath() {
        if (path == null) { // implies parent != null
            path = PathUtils.concat(parent.getPath(), name);
        }
        return path;
    }

}
