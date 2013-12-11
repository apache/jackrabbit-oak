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

package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;

/**
 * Commit info instances associate some meta data with a commit.
 */
public class CommitInfo {

    public static final String OAK_UNKNOWN = "oak:unknown";

    private final String sessionId;

    private final String userId;

    private final PermissionProvider permissionProvider;

    private final String message;

    private final long date = System.currentTimeMillis();

    private final MoveTracker moveTracker;

    /**
     * Creates a commit info for the given session and user.
     *
     * @param sessionId session identifier
     * @param userId The user id.
     * @param permissionProvider The permission provider associated with the
     * root that is committing changes.
     * @param moveTracker Information regarding move operations associated with this commit.
     * @param message message attached to this commit, or {@code null}
     */
    public CommitInfo(@Nonnull String sessionId, @Nullable String userId,
                      @Nonnull PermissionProvider permissionProvider,
                      @Nonnull MoveTracker moveTracker, @Nullable String message) {
        this.sessionId = checkNotNull(sessionId);
        this.userId = (userId == null) ? OAK_UNKNOWN : userId;
        this.permissionProvider = checkNotNull(permissionProvider);
        this.moveTracker = checkNotNull(moveTracker);
        this.message = message;
    }

    /**
     * @return  id of the committing session
     */
    @Nonnull
    public String getSessionId() {
        return sessionId;
    }

    /**
     * @return  user id of the committing user
     */
    @Nonnull
    public String getUserId() {
        return userId;
    }

    @Nonnull
    public MoveTracker getMoveTracker() {
        return moveTracker;
    }

    @Nonnull
    public PermissionProvider getPermissionProvider() {
        return permissionProvider;
    }

    /**
     * @return message attached to this commit
     */
    @CheckForNull
    public String getMessage() {
        return message;
    }

    /**
     * @return  time stamp
     */
    public long getDate() {
        return date;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("sessionId", sessionId)
                .add("userId", userId)
                .add("userData", message)
                .add("date", date)
                .toString();
    }
}
