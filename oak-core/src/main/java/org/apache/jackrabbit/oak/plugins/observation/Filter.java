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
package org.apache.jackrabbit.oak.plugins.observation;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * A filter is used by the FilteringObserver to decide whether or not a content
 * change should be forwarded.
 */
public interface Filter {

    /**
     * Whether or not to exclude a particular content change from being
     * forwarded to downstream observers.
     * 
     * @param root
     *            the new root state
     * @param info
     *            the associated CommitInfo
     * @return true to exclude this content change (not forward), false to
     *         include it (forward)
     */
    boolean excludes(@NotNull NodeState root, @NotNull CommitInfo info);

}
