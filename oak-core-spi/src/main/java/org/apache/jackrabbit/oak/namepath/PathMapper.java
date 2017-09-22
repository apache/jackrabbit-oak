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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * {@code PathMapper} instances provide methods for mapping paths from their JCR
 * string representation to their Oak representation and vice versa.
 *
 * The Oak representation of a path consists of a forward slash followed by the
 * names of the respective items in the {@link org.apache.jackrabbit.oak.api.Tree}
 * separated by forward slashes.
 */
public interface PathMapper {

    /**
     * Returns the Oak path for the given JCR path, or {@code null} if no
     * such mapping exists because the given JCR path contains a name element
     * with an unknown namespace URI or prefix, or is otherwise invalid.
     *
     * @param jcrPath JCR path
     * @return Oak path, or {@code null}
     */
    @CheckForNull
    String getOakPath(String jcrPath);

    /**
     * Returns the JCR path for the given Oak path. The given path is
     * expected to have come from a valid Oak repository that contains
     * only valid paths whose name elements only use proper namespace
     * mappings. If that's not the case, either a programming error or
     * a repository corruption has occurred and an appropriate unchecked
     * exception gets thrown.
     *
     * @param oakPath Oak path
     * @return JCR path
     */
    @Nonnull
    String getJcrPath(String oakPath);

}
