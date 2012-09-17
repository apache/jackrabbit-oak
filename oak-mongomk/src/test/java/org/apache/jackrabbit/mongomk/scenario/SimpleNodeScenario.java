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

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.command.CommitCommandMongo;
import org.apache.jackrabbit.mongomk.impl.model.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.AddPropertyInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.CommitImpl;
import org.apache.jackrabbit.mongomk.impl.model.RemoveNodeInstructionImpl;

/**
 * Creates a defined scenario in {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class SimpleNodeScenario { // TODO this should be refactored to use class rules

    private final MongoConnection mongoConnection;

    /**
     * Constructs a new {@code SimpleNodeScenario}.
     *
     * @param mongoConnection
     *            The {@link MongoConnection}.
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
     * @throws Exception
     *             If an error occurred.
     */
    public String create() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/a", "b"));
        instructions.add(new AddNodeInstructionImpl("/a", "c"));
        instructions.add(new AddPropertyInstructionImpl("/a", "int", 1));
        instructions.add(new AddPropertyInstructionImpl("/a/b", "string", "foo"));
        instructions.add(new AddPropertyInstructionImpl("/a/c", "bool", true));

        Commit commit = new CommitImpl("This is the simple node scenario with nodes /, /a, /a/b, /a/c", "/",
                "+a : { \"int\" : 1 , \"b\" : { \"string\" : \"foo\" } , \"c\" : { \"bool\" : true } } }", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        return revisionId;
    }

    public String addChildrenToA(int count) throws Exception {
        String revisionId = null;
        for (int i = 1; i <= count; i++) {
            List<Instruction> instructions = new LinkedList<Instruction>();
            instructions.add(new AddNodeInstructionImpl("/a", "child" + i));
            Commit commit = new CommitImpl("Add child" + i, "/a", "TODO", instructions);
            CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
            revisionId = command.execute();
        }
        return revisionId;
    }

    /**
     * Deletes the a node.
     *
     * <pre>
     * &quot;-a&quot;
     * </pre>
     *
     * @return The {@link RevisionId}.
     * @throws Exception
     *             If an error occurred.
     */
    public String delete_A() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new RemoveNodeInstructionImpl("/", "a"));

        Commit commit = new CommitImpl("This is a commit with deleted /a", "/", "-a", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        return revisionId;
    }

    public String delete_B() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new RemoveNodeInstructionImpl("/a", "b"));
        Commit commit = new CommitImpl("This is a commit with deleted /a/b", "/a", "-b", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        return command.execute();
    }

    /**
     * Updates the following nodes:
     *
     * <pre>
     * TBD
     * </pre>
     *
     * @return The {@link RevisionId}.
     * @throws Exception
     *             If an error occurred.
     */
    public String update_A_and_add_D_and_E() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/a", "d"));
        instructions.add(new AddNodeInstructionImpl("/a/b", "e"));
        instructions.add(new AddPropertyInstructionImpl("/a", "double", 0.123D));
        instructions.add(new AddPropertyInstructionImpl("/a/d", "null", null));
        instructions.add(new AddPropertyInstructionImpl("/a/b/e", "array", new Object[] { 123, null, 123.456D,
                "for:bar", Boolean.TRUE }));

        Commit commit = new CommitImpl("This is a commit with updated /a and added /a/d and /a/b/e", "", "TODO",
                instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        return revisionId;
    }
}
