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
package org.apache.jackrabbit.mongomk.impl.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.jackrabbit.mongomk.api.instruction.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Commit;

import com.mongodb.BasicDBObject;

/**
 * The {@code MongoDB} representation of a commit.
 */
public class MongoCommit extends BasicDBObject implements Commit {

    public static final String KEY_AFFECTED_PATH = "affPaths";
    public static final String KEY_BASE_REVISION_ID = "baseRevId";
    public static final String KEY_BRANCH_ID = "branchId";
    public static final String KEY_DIFF = "diff";
    public static final String KEY_FAILED = "failed";
    public static final String KEY_MESSAGE = "msg";
    public static final String KEY_PATH = "path";
    public static final String KEY_REVISION_ID = "revId";
    public static final String KEY_TIMESTAMP = "ts";

    private final List<Instruction> instructions;
    private SortedSet<String> affectedPaths;

    private static final long serialVersionUID = 6656294757102309827L;

    /**
     * Default constructor. Needed for MongoDB serialization.
     */
    public MongoCommit() {
        instructions = new LinkedList<Instruction>();
        setTimestamp(new Date().getTime());
    }

    //--------------------------------------------------------------------------
    //
    // These properties are persisted to MongoDB
    //
    //--------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public SortedSet<String> getAffectedPaths() {
        if (affectedPaths == null) {
            affectedPaths = new TreeSet<String>(
                    inflate((byte[]) get(KEY_AFFECTED_PATH)));
        }
        return Collections.unmodifiableSortedSet(affectedPaths);
    }

    public void setAffectedPaths(Set<String> affectedPaths) {
        this.affectedPaths = new TreeSet<String>(affectedPaths);
        put(KEY_AFFECTED_PATH, compress(this.affectedPaths));
    }

    @Override
    public Long getBaseRevisionId() {
        return containsField(KEY_BASE_REVISION_ID)? getLong(KEY_BASE_REVISION_ID) : null;
    }

    public void setBaseRevisionId(Long baseRevisionId) {
        if (baseRevisionId == null) {
            removeField(KEY_BASE_REVISION_ID);
        } else {
            put(KEY_BASE_REVISION_ID, baseRevisionId);
        }
    }

    @Override
    public String getBranchId() {
        return getString(KEY_BRANCH_ID);
    }

    public void setBranchId(String branchId) {
        put(KEY_BRANCH_ID, branchId);
    }

    @Override
    public String getDiff() {
        List<String> data = inflate((byte[]) get(KEY_DIFF));
        if (data == null || data.isEmpty()) {
            return null;
        } else {
            return data.get(0);
        }
    }

    public void setDiff(String diff) {
        put(KEY_DIFF, compress(Collections.singleton(diff)));
    }

    public boolean isFailed() {
        return getBoolean(KEY_FAILED);
    }

    public void setFailed() {
        put(KEY_FAILED, Boolean.TRUE);
    }

    @Override
    public String getMessage() {
        return getString(KEY_MESSAGE);
    }

    public void setMessage(String message) {
        put(KEY_MESSAGE, message);
    }

    @Override
    public String getPath() {
        return getString(KEY_PATH);
    }

    public void setPath(String path) {
        put(KEY_PATH, path);
    }

    @Override
    public Long getRevisionId() {
        return containsField(KEY_REVISION_ID)? getLong(KEY_REVISION_ID) : null;
    }

    @Override
    public void setRevisionId(Long revisionId) {
        put(KEY_REVISION_ID, revisionId);
    }

    @Override
    public Long getTimestamp() {
        return getLong(KEY_TIMESTAMP);
    }

    public void setTimestamp(Long timestamp) {
        put(KEY_TIMESTAMP, timestamp);
    }

    //--------------------------------------------------------------------------
    //
    // These properties are used to keep track but not persisted to MongoDB
    //
    //--------------------------------------------------------------------------

    /**
     * Adds the given {@link Instruction}.
     *
     * @param instruction The {@code Instruction}.
     */
    public void addInstruction(Instruction instruction) {
        instructions.add(instruction);
    }

    @Override
    public List<Instruction> getInstructions() {
        return Collections.unmodifiableList(instructions);
    }

    //-----------------------------< internal >---------------------------------

    private List<String> inflate(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        List<String> data = new ArrayList<String>();
        try {
            DataInputStream in = new DataInputStream(new GZIPInputStream(
                    new ByteArrayInputStream(bytes)));
            int num = in.readInt();
            for (int i = 0; i < num; i++) {
                int len = in.readInt();
                byte[] buffer = new byte[len];
                in.readFully(buffer);
                data.add(new String(buffer, "UTF-8"));
            }
        } catch (IOException e) {
            // FIXME: shoulnd't actually happen with byte[] and UTF-8
            throw new RuntimeException(e);
        }
        return data;
    }

    private byte[] compress(Collection<String> strings) {
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            OutputStream out = new GZIPOutputStream(byteOut);
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeInt(strings.size());
            for (String s : strings) {
                byte[] buffer = s.getBytes("UTF-8");
                dataOut.writeInt(buffer.length);
                dataOut.write(buffer);
            }
            dataOut.close();
            out.close();
            return byteOut.toByteArray();
        } catch (IOException e) {
            // FIXME: shoulnd't actually happen with byte[] and UTF-8
            throw new RuntimeException(e);
        }
    }
}