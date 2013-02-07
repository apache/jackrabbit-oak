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
package org.apache.jackrabbit.mongomk.impl.json;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
* <code>NormalizingJsopHandler</code>...
*/
public class NormalizingJsopHandler extends DefaultJsopHandler {

    private final StringBuilder builder = new StringBuilder();
    private final Deque<String> commaStack = new ArrayDeque<String>(Collections.singleton(""));
    private final Deque<String> pathStack;
    private final String path;

    public NormalizingJsopHandler() {
        this("/");
    }

    public NormalizingJsopHandler(String path) {
        this.path = path;
        pathStack = new ArrayDeque<String>(Collections.singleton(path));
    }

    public String getDiff() {
        scopeFor(path);
        return builder.toString();
    }

    @Override
    public void nodeAdded(String parentPath, String name) {
        String relPath = scopeFor(PathUtils.concat(parentPath, name));
        if (pathStack.size() == 1) {
            builder.append("+");
        } else {
            maybeAppendComma();
        }
        if (relPath.length() > 0) {
            pathStack.addLast(relPath);
            commaStack.addLast("");
        }
        builder.append(JsopBuilder.encode(relPath));
        builder.append(":{");
        resetComma();
    }

    @Override
    public void nodeCopied(String rootPath,
                           String oldPath,
                           String newPath) {
        scopeFor(path);
        builder.append("*");
        builder.append(JsopBuilder.encode(relativize(path, oldPath)));
        builder.append(":");
        builder.append(JsopBuilder.encode(relativize(path, newPath)));
    }

    @Override
    public void nodeMoved(String rootPath, String oldPath, String newPath) {
        scopeFor(path);
        builder.append(">");
        builder.append(JsopBuilder.encode(relativize(path, oldPath)));
        builder.append(":");
        builder.append(JsopBuilder.encode(relativize(path, newPath)));
    }

    @Override
    public void nodeRemoved(String parentPath, String name) {
        scopeFor(path);
        builder.append("-");
        builder.append(JsopBuilder.encode(relativize(path, concatPath(parentPath, name))));
    }

    @Override
    public void propertySet(String path, String key, Object value, String rawValue) {
        String relPath = scopeFor(path);
        if (pathStack.size() == 1) {
            builder.append("^");
        } else {
            maybeAppendComma();
        }
        builder.append(JsopBuilder.encode(concatPath(relPath, key)));
        builder.append(":");
        builder.append(rawValue);
    }

    /**
     * Opens a new scope for the given path relative to the current path.
     * @param path the path of the new scope.
     * @return the remaining relative path needed for the given scope path.
     */
    private String scopeFor(String path) {
        // close brackets until path stack is the root, the same as path or
        // an ancestor of path
        while (pathStack.size() > 1
                && !path.equals(getCurrentPath())
                && !PathUtils.isAncestor(getCurrentPath(), path)) {
            pathStack.removeLast();
            commaStack.removeLast();
            builder.append("}");
        }
        // remaining path for scope
        return relativize(getCurrentPath(), path);
    }

    private String getCurrentPath() {
        String path = "";
        for (String element : pathStack) {
            path = PathUtils.concat(path, element);
        }
        return path;
    }

    private String concatPath(String parent, String child) {
        if (parent.length() == 0) {
            return child;
        } else {
            return PathUtils.concat(parent, child);
        }
    }

    private void resetComma() {
        commaStack.removeLast();
        commaStack.addLast("");
    }

    private void maybeAppendComma() {
        builder.append(commaStack.removeLast());
        commaStack.addLast(",");
    }

    private String relativize(String parentPath, String path) {
        return parentPath.isEmpty()? path : PathUtils.relativize(parentPath, path);
    }
}