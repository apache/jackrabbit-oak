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

import java.util.Iterator;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.AuthInfo;

import static com.google.common.base.Objects.toStringHelper;

/**
 * Commit info instances associate some meta data with a commit.
 */
public class CommitInfo {

    public static final String OAK_UNKNOWN = "oak:unknown";

    private final String sessionId;

    private final Subject subject;

    private final String message;

    private final long date = System.currentTimeMillis();

    private final MoveInfo moveInfo;

    /**
     * Creates a commit info for the given session and user.
     *
     * @param sessionId session identifier
     * @param subject Subject identifying the user
     * @param moveInfo Information regarding move operations associated with this commit.
     * @param message message attached to this commit, or {@code null}
     */
    public CommitInfo(@Nonnull String sessionId, @Nonnull Subject subject,
                      @Nonnull MoveInfo moveInfo, @Nullable String message) {
        this.sessionId = sessionId;
        this.subject = subject;
        this.message = message;
        this.moveInfo = moveInfo;
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
        Iterator<AuthInfo> it = subject.getPublicCredentials(AuthInfo.class).iterator();
        String userId = null;
        if (it.hasNext()) {
            userId = it.next().getUserID();
        }
        return (userId == null) ? OAK_UNKNOWN : userId;
    }

    @Nonnull
    public Subject getSubject() {
        return subject;
    }

    @Nonnull
    public MoveInfo getMoveInfo() {
        return moveInfo;
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
                .add("userId", getUserId())
                .add("userData", message)
                .add("date", date)
                .add("moveInfo", moveInfo)
                .toString();
    }

}
