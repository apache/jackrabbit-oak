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

package org.apache.jackrabbit.oak.remote;

import java.util.Map;

/**
 * This interface is a recursive data structure representing a view on a
 * repository tree.
 * <p>
 * The purpose of the remote tree is not to represent exactly the content as
 * stored in the repository, but to provide a view on that content fulfilling
 * the filtering options provided by the client when the tree was accessed.
 */
public interface RemoteTree {

    /**
     * Read the properties associated to the root of this remote tree. The root
     * of this remote tree is represented by the instance of {@code RemoteTree}
     * this method is invoked on.
     *
     * @return the properties associated to the root of this remote tree.
     */
    Map<String, RemoteValue> getProperties();

    /**
     * Read the children associated to the root of this remote tree. The root of
     * this remote tree is represented by the instance of {@code RemoteTree}
     * this method is invoked on. The children of this remote tree are
     * themselves remote trees.
     * <p>
     * The remote tree may be truncated at some point (e.g. to avoid very deep
     * remote trees to be returned), and this is the reason why the values of
     * this {@code Map} can be {@code null}. When a {@code null} value is met,
     * the consumer of this interface must assume that there is another subtree
     * rooted under the corresponding key, but it is not returned to fulfill the
     * filtering options provided when this tree was read.
     *
     * @return The children associated to the root of this remote tree.
     */
    Map<String, RemoteTree> getChildren();

    /**
     * Return a flag to indicate that this remote tree actually has more
     * children than the one returned by {@link #getChildren()}.
     * <p>
     * This flag is important when the repository tree is read using very strict
     * filtering options regarding the maximum number of children to return. If
     * this method returns {@code true}, a consumer of this interface must
     * assume that there are more children than the one attached to the root of
     * this tree. They could be retrieved by varying the relevant filtering
     * options and performing another read for this subtree.
     *
     * @return {@code true} if this remote tree is not exposing the full set of
     * children as stored in the repository, {@code false} otherwise.
     */
    boolean hasMoreChildren();

}
