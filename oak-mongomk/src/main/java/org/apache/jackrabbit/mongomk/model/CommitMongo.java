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
package org.apache.jackrabbit.mongomk.model;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.BasicDBObject;

/**
 * The {@code MongoDB} representation of a commit.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class CommitMongo extends BasicDBObject {

    public static final String KEY_AFFECTED_PATH = "affPaths";
    public static final String KEY_BASE_REVISION_ID = "baseRevId";
    public static final String KEY_DIFF = "diff";
    public static final String KEY_FAILED = "failed";
    public static final String KEY_MESSAGE = "msg";
    public static final String KEY_PATH = "path";
    public static final String KEY_REVISION_ID = "revId";
    public static final String KEY_TIMESTAMP = "ts";
    private static final long serialVersionUID = 6656294757102309827L;

    public static CommitMongo fromCommit(Commit commit) {
        CommitMongo commitMongo = new CommitMongo();

        String message = commit.getMessage();
        commitMongo.setMessage(message);

        String path = commit.getPath();
        commitMongo.setPath(path);

        String diff = commit.getDiff();
        commitMongo.setDiff(diff);

        Long revisionId = commit.getRevisionId();
        if (revisionId != null) {
            commitMongo.setRevisionId(revisionId);
        }

        commitMongo.setTimestamp(commit.getTimestamp());

        Set<String> affectedPaths = new HashSet<String>();
        for (Instruction instruction : commit.getInstructions()) {
            affectedPaths.add(instruction.getPath());

            if (instruction instanceof AddNodeInstruction) {
                affectedPaths.add(PathUtils.getParentPath(instruction.getPath()));
            }
        }
        commitMongo.setAffectedPaths(new LinkedList<String>(affectedPaths));

        return commitMongo;
    }

    public CommitMongo() {
        setTimestamp(new Date().getTime());
    }

    @SuppressWarnings("unchecked")
    public List<String> getAffectedPaths() {
        return (List<String>) get(KEY_AFFECTED_PATH);
    }

    public long getBaseRevisionId() {
        return getLong(KEY_BASE_REVISION_ID);
    }

    public String getDiff() {
        return getString(KEY_DIFF);
    }

    public String getMessage() {
        return getString(KEY_MESSAGE);
    }

    public String getPath() {
        return getString(KEY_PATH);
    }

    public long getRevisionId() {
        return getLong(KEY_REVISION_ID);
    }

    public boolean hasFailed() {
        return getBoolean(KEY_FAILED);
    }

    public void setAffectedPaths(List<String> affectedPaths) {
        put(KEY_AFFECTED_PATH, affectedPaths);
    }

    public void setBaseRevId(long baseRevisionId) {
        put(KEY_BASE_REVISION_ID, baseRevisionId);
    }

    public void setDiff(String diff) {
        put(KEY_DIFF, diff);
    }

    public void setFailed() {
        put(KEY_FAILED, Boolean.TRUE);
    }

    public void setMessage(String message) {
        put(KEY_MESSAGE, message);
    }

    public void setPath(String path) {
        put(KEY_PATH, path);
    }

    public void setRevisionId(long revisionId) {
        put(KEY_REVISION_ID, revisionId);
    }

    public void setTimestamp(long timestamp) {
        put(KEY_TIMESTAMP, timestamp);
    }

    public Long getTimestamp() {
        return getLong(KEY_TIMESTAMP);
    }
}