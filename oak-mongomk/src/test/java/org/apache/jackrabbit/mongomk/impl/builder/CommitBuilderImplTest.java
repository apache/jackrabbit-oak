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
package org.apache.jackrabbit.mongomk.impl.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import junit.framework.Assert;

import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.CopyNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.MoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.RemoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.SetPropertyInstruction;
import org.apache.jackrabbit.mongomk.impl.InstructionAssert;
import org.apache.jackrabbit.mongomk.impl.builder.CommitBuilder;
import org.junit.Test;


/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class CommitBuilderImplTest {

    private static final String MESSAGE = "This is a simple commit";
    private static final String ROOT = "/";

    @Test
    public void testSimpleAdd() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("+\"a\" : { \"int\" : 1 } \n");
        sb.append("+\"a/b\" : { \"string\" : \"foo\" } \n");
        sb.append("+\"a/c\" : { \"bool\" : true }");

        Commit commit = this.buildAndAssertCommit(sb.toString());

        List<Instruction> instructions = commit.getInstructions();
        Assert.assertEquals(6, instructions.size());
        InstructionAssert.assertAddNodeInstruction((AddNodeInstruction) instructions.get(0), "/a");
        InstructionAssert.assertAddPropertyInstruction((AddPropertyInstruction) instructions.get(1), "/a", "int", 1);
        InstructionAssert.assertAddNodeInstruction((AddNodeInstruction) instructions.get(2), "/a/b");
        InstructionAssert.assertAddPropertyInstruction((AddPropertyInstruction) instructions.get(3), "/a/b", "string",
                "foo");
        InstructionAssert.assertAddNodeInstruction((AddNodeInstruction) instructions.get(4), "/a/c");
        InstructionAssert.assertAddPropertyInstruction((AddPropertyInstruction) instructions.get(5), "/a/c", "bool",
                true);
    }

    @Test
    public void testSimpleCopy() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("*\"a\" : \"b\"\n");
        sb.append("*\"a/b\" : \"a/c\"\n");

        Commit commit = this.buildAndAssertCommit(sb.toString());
        List<Instruction> instructions = commit.getInstructions();
        assertEquals(2, instructions.size());
        InstructionAssert.assertCopyNodeInstruction((CopyNodeInstruction) instructions.get(0), "/", "/a", "/b");
        InstructionAssert.assertCopyNodeInstruction((CopyNodeInstruction) instructions.get(1), "/", "/a/b", "/a/c");
    }

    @Test
    public void testSimpleMove() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(">\"a\" : \"b\"\n");
        sb.append(">\"a/b\" : \"a/c\"\n");

        Commit commit = this.buildAndAssertCommit(sb.toString());
        List<Instruction> instructions = commit.getInstructions();
        assertEquals(2, instructions.size());
        InstructionAssert.assertMoveNodeInstruction((MoveNodeInstruction) instructions.get(0), "/", "/a", "/b");
        InstructionAssert.assertMoveNodeInstruction((MoveNodeInstruction) instructions.get(1), "/", "/a/b", "/a/c");
    }

    @Test
    public void testSimpleRemove() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("-\"a\"");
        // TODO properties

        Commit commit = this.buildAndAssertCommit(sb.toString());

        List<Instruction> instructions = commit.getInstructions();
        assertEquals(1, instructions.size());
        InstructionAssert.assertRemoveNodeInstruction((RemoveNodeInstruction) instructions.get(0), "/a");
    }

    @Test
    public void testSimpleSet() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("^\"a\" : \"b\"\n");

        Commit commit = this.buildAndAssertCommit(sb.toString());

        List<Instruction> instructions = commit.getInstructions();
        assertEquals(1, instructions.size());
        InstructionAssert.assertSetPropertyInstruction((SetPropertyInstruction) instructions.get(0), "/", "a", "b");
    }

    private Commit buildAndAssertCommit(String commitString) throws Exception {
        Commit commit = CommitBuilder.build(ROOT, commitString, MESSAGE);

        assertNotNull(commit);
        assertEquals(MESSAGE, commit.getMessage());
        assertNull(commit.getRevisionId());
        return commit;
    }
}
