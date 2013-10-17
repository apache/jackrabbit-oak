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

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Provider for a {@link Editor} that amends each commit with
 * a commit info record. That record is stored in a child node
 * named {@link #COMMIT_INFO} of the root node.
 */
public class CommitInfoEditorProvider implements EditorProvider {

    /**
     * Node name for the commit info record
     */
    public static final String COMMIT_INFO = ":commit-info";

    /**
     * Name of the property containing the id of the session that committed
     * this revision.
     * <p>
     * In a clustered environment this property might contain a synthesised
     * value if the respective revision is the result of a cluster sync.
     */
    public static final String SESSION_ID = "session-id";

    /**
     * Name of the property containing the id of the user that committed
     * this revision.
     * <p>
     * In a clustered environment this property might contain a synthesised
     * value if the respective revision is the result of a cluster sync.
     */
    public static final String USER_ID = "user-id";

    /**
     * Name of the property containing the time stamp when this revision was
     * committed.
     */
    public static final String TIME_STAMP = "time-stamp";

    private final String sessionId;
    private final String userId;

    public CommitInfoEditorProvider(@Nonnull String sessionId, String userID) {
        this.sessionId = checkNotNull(sessionId);
        this.userId = userID;
    }

    @Override
    public Editor getRootEditor(NodeState before, NodeState after, final NodeBuilder builder) {
        return new DefaultEditor() {
            @Override
            public void enter(NodeState before, NodeState after) {
                NodeBuilder commitInfo = builder.setChildNode(COMMIT_INFO);
                commitInfo.setProperty(USER_ID, String.valueOf(userId));
                commitInfo.setProperty(SESSION_ID, sessionId);
                commitInfo.setProperty(TIME_STAMP, System.currentTimeMillis());
            }
        };
    }
}
