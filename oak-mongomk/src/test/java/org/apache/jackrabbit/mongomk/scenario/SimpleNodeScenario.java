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
package org.apache.jackrabbit.mongomk.scenario;

import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.command.CommitCommandMongo;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.builder.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.model.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.AddPropertyInstructionImpl;

/**
 * Creates a defined scenario in {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class SimpleNodeScenario {

    private final MongoConnection mongoConnection;

    /**
     * Constructs a new {@code SimpleNodeScenario}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     */
    public SimpleNodeScenario(MongoConnection mongoConnection) {
        this.mongoConnection = mongoConnection;
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
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        return command.execute();
    }

    public Long addChildrenToA(int count) throws Exception {
        Long revisionId = null;
        for (int i = 1; i <= count; i++) {
            Commit commit = CommitBuilder.build("/a", "+\"child" + i + "\" : {}", "Add child" + i);
            CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
            revisionId = command.execute();
        }
        return revisionId;
    }

    public Long delete_A() throws Exception {
        Commit commit = CommitBuilder.build("/", "-\"a\"", "This is a commit with deleted /a");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        return command.execute();
    }

    public Long delete_B() throws Exception {
        Commit commit = CommitBuilder.build("/a", "-\"b\"", "This is a commit with deleted /a/b");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        return command.execute();
    }

    public Long update_A_and_add_D_and_E() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/a", "d"));
        instructions.add(new AddNodeInstructionImpl("/a/b", "e"));
        instructions.add(new AddPropertyInstructionImpl("/a", "double", 0.123D));
        instructions.add(new AddPropertyInstructionImpl("/a/d", "null", null));
        instructions.add(new AddPropertyInstructionImpl("/a/b/e", "array", new Object[] { 123, null, 123.456D,
                "for:bar", Boolean.TRUE }));

        StringBuilder diff = new StringBuilder();
        diff.append("+\"a/d\" : {}");
        diff.append("+\"a/b/e\" : {}");
        diff.append("+\"a/double\" : 0.123");
        diff.append("+\"a/d/null\" :  null");
        diff.append("+\"a/b/e/array\" : [ 123, null, 123.456, \"for:bar\", true ]");
        Commit commit = CommitBuilder.build("/", diff.toString(),
                "This is a commit with updated /a and added /a/d and /a/b/e");

        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        return command.execute();
    }
}
