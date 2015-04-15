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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ContentChangeInfo {
    private final NodeState before;
    private final NodeState after;
    private final CommitInfo commitInfo;

    public ContentChangeInfo(@Nonnull NodeState before,
                             @Nonnull NodeState after,
                             @Nullable CommitInfo commitInfo) {
        this.before = before;
        this.after = after;
        this.commitInfo = commitInfo;
    }

    public NodeState getBefore() {
        return before;
    }

    public NodeState getAfter() {
        return after;
    }

    public boolean isLocalChange(){
        return commitInfo != null;
    }
}
