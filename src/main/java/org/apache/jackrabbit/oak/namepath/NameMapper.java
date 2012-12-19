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
 * TODO document
 */
public interface NameMapper {

    /**
     * Returns the Oak name for the given JCR name, or {@code null} if no
     * such mapping exists because the given JCR name contains an unknown
     * namespace URI or prefix, or is otherwise invalid.
     *
     * @param jcrName JCR name
     * @return Oak name, or {@code null}
     */
    @CheckForNull
    String getOakName(@Nonnull String jcrName);

    /**
     * Returns whether the mapper has prefix remappings; when there aren't
     * any, prefixed names do not need to be converted at all
     * 
     * @return {@code true} if prefixes have been remapped
     */
    boolean hasSessionLocalMappings();

    /**
     * Returns the JCR name for the given Oak name. The given name is
     * expected to have come from a valid Oak repository that contains
     * only valid names with proper namespace mappings. If that's not
     * the case, either a programming error or a repository corruption
     * has occurred and an appropriate unchecked exception gets thrown.
     *
     * @param oakName Oak name
     * @return JCR name
     */
    @Nonnull
    String getJcrName(@Nonnull String oakName);

}
