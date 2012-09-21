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

/**
 * The event callback of the parser.
 *
 * <p>
 * Each event callback has an empty default implementation. An implementor may choose the appropriate methods to
 * overwrite.
 * </p>
 */
public class DefaultJsopHandler {

    /**
     * Event: A node has been added.
     *
     * @param parentPath The path where the node was added to.
     * @param name The name of the added node.
     */
    public void nodeAdded(String parentPath, String name) {
        // No-op
    }

    /**
     * Event: A node was copied.
     *
     * @param rootPath The root path where the copy took place.
     * @param oldPath The old path of the node (relative to the root path).
     * @param newPath The new path of the node (relative to the root path).
     */
    public void nodeCopied(String rootPath, String oldPath, String newPath) {
        // No-op
    }

    /**
     * Event: A node was moved.
     *
     * @param rootPath The root path where the copy took place.
     * @param oldPath The old path of the node (relative to the root path).
     * @param newPath The new path of the node (relative to the root path).
     */
    public void nodeMoved(String rootPath, String oldPath, String newPath) {
        // No-op
    }

    /**
     * Event: A node was removed.
     *
     * @param parentPath The path where the node was removed from.
     * @param name The name of the node.
     */
    public void nodeRemoved(String parentPath, String name) {
        // No-op
    }

    /**
     * Event: A property was added.
     *
     * @param path The path of the node where the property was added.
     * @param key The key of the property.
     * @param value The value of the property.
     */
    public void propertyAdded(String path, String key, Object value) {
        // No-op
    }

    /**
     * Event: A property was set.
     *
     * @param path The path of the node where the property was set.
     * @param key The key of the property.
     * @param value The value of the property.
     */
    public void propertySet(String path, String key, Object value) {
        // No-op
    }
}
