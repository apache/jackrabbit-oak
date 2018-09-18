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
package org.apache.jackrabbit.oak.plugins.tree;

/**
 * Allows to distinguish different types of trees based on their name, ancestry
 * or primary type. Currently the following types are supported:
 *
 * <ul>
 *     <li>{@link #DEFAULT}: the default type for trees that don't match any of the following types.</li>
 *     <li>{@link #ACCESS_CONTROL}: A tree that stores access control content
 *     and requires special access {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#READ_ACCESS_CONTROL permissions}.</li>
 *     <li>{@link #HIDDEN}: a hidden tree whose name starts with ":".
 *     Please note that the whole subtree of a hidden node is considered hidden.</li>
 *     <li>{@link #INTERNAL}: repository internal content that is not hidden (e.g. permission store)</li>
 *     <li>{@link #VERSION}: if a given tree is located within
 *     any of the version related stores defined by JSR 283. Depending on the
 *     permission evaluation implementation those items require special treatment.</li>
 * </ul>
 */
public enum TreeType {

    /**
     * Regular trees that don't match any of the other types
     */
    DEFAULT,

    /**
     * Access control content
     */
    ACCESS_CONTROL,

    /**
     * Hidden trees starting at a tree whose name starts with ':'.
     * @see org.apache.jackrabbit.oak.spi.state.NodeStateUtils#isHidden(String)
     */
    HIDDEN,

    /**
     * Repository internal content that is not hidden (e.g. permission store)
     */
    INTERNAL,

    /**
     * Trees within the version, activity or configuration storage
     */
    VERSION
}