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

import org.apache.jackrabbit.mongomk.api.instruction.Instruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.BasicDBObject;

/**
 * FIXME - Should look into merging this and {@code CommitImpl}
 *
 * The {@code MongoDB} representation of a commit.
 */
public class CommitMongo extends BasicDBObject {

    public static final String KEY_AFFECTED_PATH = "affPaths";
    public static final String KEY_BASE_REVISION_ID = "baseRevId";
    public static final String KEY_BRANCH_ID = "branchId";
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

        String branchId = commit.getBranchId();
        if (branchId != null) {
            commitMongo.setBranchId(branchId);
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

    public void setAffectedPaths(List<String> affectedPaths) {
        put(KEY_AFFECTED_PATH, affectedPaths);
    }

    public long getBaseRevId() {
        return getLong(KEY_BASE_REVISION_ID);
    }

    public void setBaseRevId(long baseRevisionId) {
        put(KEY_BASE_REVISION_ID, baseRevisionId);
    }

    public String getBranchId() {
        return getString(KEY_BRANCH_ID);
    }

    public void setBranchId(String branchId) {
        put(KEY_BRANCH_ID, branchId);
    }

    public String getDiff() {
        return getString(KEY_DIFF);
    }

    public void setDiff(String diff) {
        put(KEY_DIFF, diff);
    }

    public String getMessage() {
        return getString(KEY_MESSAGE);
    }

    public void setMessage(String message) {
        put(KEY_MESSAGE, message);
    }

    public String getPath() {
        return getString(KEY_PATH);
    }

    public void setPath(String path) {
        put(KEY_PATH, path);
    }

    public long getRevisionId() {
        return getLong(KEY_REVISION_ID);
    }

    public void setRevisionId(long revisionId) {
        put(KEY_REVISION_ID, revisionId);
    }

    public boolean isFailed() {
        return getBoolean(KEY_FAILED);
    }

    public void setFailed() {
        put(KEY_FAILED, Boolean.TRUE);
    }

    public Long getTimestamp() {
        return getLong(KEY_TIMESTAMP);
    }

    public void setTimestamp(long timestamp) {
        put(KEY_TIMESTAMP, timestamp);
    }
}