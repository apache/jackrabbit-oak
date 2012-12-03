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
package org.apache.jackrabbit.mongomk.impl.action;

import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.command.CommitCommandNew;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;

/**
 * Creates a defined scenario in {@code MongoDB}.
 */
public class SimpleNodeScenario {

    private final MongoNodeStore nodeStore;

    /**
     * Constructs a new {@code SimpleNodeScenario}.
     *
     * @param nodeStore Node store.
     */
    public SimpleNodeScenario(MongoNodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    /**
     * Creates the following nodes:
     *
     * <pre>
     * &quot;+a : { \&quot;int\&quot; : 1 , \&quot;b\&quot; : { \&quot;string\&quot; : \&quot;foo\&quot; } , \&quot;c\&quot; : { \&quot;bool\&quot; : true } } }&quot;
     * </pre>
     *
     * @return The {@link RevisionId}.
     * @throws Exception If an error occurred.
     */
    public Long create() throws Exception {
        Commit commit = CommitBuilder.build("/",
                "+\"a\" : { \"int\" : 1 , \"b\" : { \"string\" : \"foo\" } , \"c\" : { \"bool\" : true } }",
                "This is the simple node scenario with nodes /, /a, /a/b, /a/c");
        return new CommitCommandNew(nodeStore, commit).execute();
    }

    public Long addChildrenToA(int count) throws Exception {
        Long revisionId = null;
        for (int i = 1; i <= count; i++) {
            Commit commit = CommitBuilder.build("/a", "+\"child" + i + "\" : {}", "Add child" + i);
            revisionId = new CommitCommandNew(nodeStore, commit).execute();
        }
        return revisionId;
    }

    public Long delete_A() throws Exception {
        Commit commit = CommitBuilder.build("/", "-\"a\"", "This is a commit with deleted /a");
        return new CommitCommandNew(nodeStore, commit).execute();
    }

    public Long delete_B() throws Exception {
        Commit commit = CommitBuilder.build("/a", "-\"b\"", "This is a commit with deleted /a/b");
        return new CommitCommandNew(nodeStore, commit).execute();
    }

    public Long update_A_and_add_D_and_E() throws Exception {
        StringBuilder diff = new StringBuilder();
        diff.append("+\"a/d\" : {}");
        diff.append("+\"a/b/e\" : {}");
        diff.append("^\"a/double\" : 0.123");
        diff.append("^\"a/d/int\" :  2");
        diff.append("^\"a/b/e/array\" : [ 123, null, 123.456, \"for:bar\", true ]");
        Commit commit = CommitBuilder.build("/", diff.toString(),
                "This is a commit with updated /a and added /a/d and /a/b/e");
        return new CommitCommandNew(nodeStore, commit).execute();
    }
}
