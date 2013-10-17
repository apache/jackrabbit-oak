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

import javax.annotation.Nonnull;

/**
 * Commit info instances associate some meta data with a commit.
 */
public class CommitInfo {
    public static final String OAK_UNKNOWN = "oak:unknown";
    public static final CommitInfo EMPTY = new CommitInfo(OAK_UNKNOWN, OAK_UNKNOWN, 0);

    private final String sessionId;
    private final String userId;
    private final long date;

    /**
     * Factory method for creating a new {@code CommitInfo} instance.
     * Both string arguments default to {@link #OAK_UNKNOWN} if {@code null}.
     *
     * @param sessionId  id of the committing session
     * @param userId  id of the committing user
     * @param date  time stamp
     * @return  a fresh {@code CommitInfo} instance
     */
    public static CommitInfo create(String sessionId, String userId, long date) {
        return new CommitInfo(
                sessionId == null ? OAK_UNKNOWN : sessionId,
                userId == null ? OAK_UNKNOWN : userId,
                date);
    }

    private CommitInfo(@Nonnull String sessionId, @Nonnull String userId, long date) {
        this.sessionId = sessionId;
        this.userId = userId;
        this.date = date;
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
                .add("date", date)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other.getClass() != this.getClass()) {
            return false;
        }

        CommitInfo that = (CommitInfo) other;
        return date == that.date
                && sessionId.equals(that.sessionId)
                && userId.equals(that.userId);
    }

    @Override
    public int hashCode() {
        int hash = sessionId.hashCode();
        hash = 31 * hash + userId.hashCode();
        return 31 * hash + (int) (date ^ (date >>> 32));
    }
}
