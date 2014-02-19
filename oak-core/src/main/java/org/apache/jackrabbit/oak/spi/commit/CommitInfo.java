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

import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Root;

/**
 * Commit info instances associate some meta data with a commit.
 */
public final class CommitInfo {

    public static final String OAK_UNKNOWN = "oak:unknown";

    /**
     * Empty commit information object. Used as a dummy object when no
     * metadata is known (or needed) about a commit.
     */
    public static final CommitInfo EMPTY =
            new CommitInfo(OAK_UNKNOWN, OAK_UNKNOWN, "/");

    private final String sessionId;

    private final String userId;

    private final long date = System.currentTimeMillis();

    private final Map<String, Object> info;

    /**
     * Creates a commit info for the given session and user.
     *
     * @param sessionId session identifier
     * @param userId The user id.
     */
    public CommitInfo(
            @Nonnull String sessionId, @Nullable String userId,
            @Nullable String message) {
        this(sessionId, userId, message, "/");
    }

    public CommitInfo(
            @Nonnull String sessionId, @Nullable String userId,
            @Nullable String message, @Nonnull String path) {
        this(sessionId, userId, ImmutableMap.<String, Object>of(
                Root.COMMIT_PATH, checkNotNull(path),
                "message", String.valueOf(message)
        ));
    }

    /**
     * Creates a commit info for the given session and user and info map.
     *
     * @param sessionId session identifier
     * @param userId The user id.
     * @param info info map
     */
    public CommitInfo(@Nonnull String sessionId, @Nullable String userId, Map<String, Object> info) {
        this.sessionId = checkNotNull(sessionId);
        this.userId = (userId == null) ? OAK_UNKNOWN : userId;
        this.info = checkNotNull(info);
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

    /**
     * @return message attached to this commit
     */
    @CheckForNull
    public String getMessage() {
        Object message = info.get("message");
        return message instanceof String ? (String) message : null;
    }

    /**
     * @return  time stamp
     */
    public long getDate() {
        return date;
    }

    /**
     * Returns the base path of this commit. All changes within this commit
     * are expected to be located within the returned path. By default this
     * is the root path, but a particular commit can declare a more specific
     * base path to indicate that only changes within that subtree should
     * be considered. Note that this value is purely informational and its
     * interpretation depends on the kinds of commit hooks and observers
     * present on the system.
     *
     * @return base path of this commit
     */
    public String getPath() {
        Object path = info.get(Root.COMMIT_PATH);
        return path instanceof String ? (String) path : "/";
    }

    /**
     * Return the info map
     * @return  info map
     */
    public Map<String, Object> getInfo() {
        return info;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof CommitInfo) {
            CommitInfo that = (CommitInfo) object;
            return sessionId.equals(that.sessionId)
                    && userId.equals(that.userId)
                    && this.date == that.date
                    && info.equals(that.info);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sessionId, userId, date, info);
    }

    @Override
    public String toString() {
        return toStringHelper(this).omitNullValues()
                .add("sessionId", sessionId)
                .add("userId", userId)
                .add("date", date)
                .add("info", info)
                .toString();
    }

}
