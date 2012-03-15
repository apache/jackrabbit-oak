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

package org.apache.jackrabbit.oak.jcr.util;


import org.apache.jackrabbit.mk.util.PathUtils;

public final class Path {
    private final String workspace;
    private final String jcrPath;

    private Path(String workspace, String jcrPath) {
        this.workspace = workspace;
        this.jcrPath = jcrPath;
    }

    public static Path create(String workspace) {
        return create(workspace, "/");
    }

    public static Path create(String workspace, String path) {
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Not an absolute path: " + path);
        }
        
        return new Path(workspace, path);
    }

    public String getWorkspace() {
        return workspace;
    }

    public String toJcrPath() {
        return jcrPath;
    }

    public String toMkPath() {
        return buildMkPath(workspace, jcrPath);
    }

    public boolean isRoot() {
        return "/".equals(jcrPath);
    }
    
    public boolean isAncestorOf(Path absPath) {
        return workspace.equals(absPath.workspace) && PathUtils.isAncestor(jcrPath, absPath.jcrPath);
    }

    public Path move(Path from, Path to) {
        if (from.isAncestorOf(this)) {
            return create(getWorkspace(), to.jcrPath + jcrPath.substring(from.jcrPath.length()));
        }
        else {
            return this;
        }
    }
    
    public Path concat(String relPath) {
        if (relPath.isEmpty()) {
            return this;
        }
        if (relPath.startsWith("/")) {
            throw new IllegalArgumentException("Not a relative path: " + relPath);
        }

        return new Path(workspace, PathUtils.concat(jcrPath, relPath));
    }

    public Path getAncestor(int depth) {
        if (depth == 0) {
            return new Path(workspace, "/");
        }

        int pos = 0;
        for (int k = 0; k < depth && pos >= 0; k++) {
            pos = PathUtils.getNextSlash(jcrPath, pos + 1);
        }

        return pos > 0
            ? new Path(workspace, jcrPath.substring(0, pos))
            : null;
    }

    public Path getParent() {
        return isRoot()
            ? null
            : new Path(workspace, PathUtils.getParentPath(jcrPath));
    }

    public String getName() {
        return isRoot()
            ? ""
            : PathUtils.getName(jcrPath);
    }

    public Iterable<String> getNames() {
        return PathUtils.elements(jcrPath);
    }

    public int getDepth() {
        return PathUtils.getDepth(jcrPath);
    }

    @Override
    public String toString() {
        return workspace + ':' + jcrPath;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Path)) {
            return false;
        }

        Path otherPath = (Path) other;

        return (jcrPath == null ? otherPath.jcrPath == null : jcrPath.equals(otherPath.jcrPath))
            && (workspace == null ? otherPath.workspace == null : workspace.equals(otherPath.workspace));

    }

    @Override
    public int hashCode() {
        return 31 * (workspace == null ? 0 : workspace.hashCode())
                  + (jcrPath == null ? 0 : jcrPath.hashCode());
    }

    //------------------------------------------< private >---

    private static String buildMkPath(String workspace, String path) {
        return '/' + ("/".equals(path)
            ? workspace
            : workspace + path);
    }

}
