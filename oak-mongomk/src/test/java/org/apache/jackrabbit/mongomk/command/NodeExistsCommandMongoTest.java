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
package org.apache.jackrabbit.mongomk.command;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.impl.model.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.CommitImpl;
import org.apache.jackrabbit.mongomk.impl.model.RemoveNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class NodeExistsCommandMongoTest extends BaseMongoTest {

	@Test
	public void simple() throws Exception {
		SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
		String revisionId = scenario.create();

		NodeExistsCommandMongo command = new NodeExistsCommandMongo(
				mongoConnection, "/a", revisionId);
		boolean exists = command.execute();
		assertTrue(exists);

		command = new NodeExistsCommandMongo(mongoConnection, "/a/b",
				revisionId);
		exists = command.execute();
		assertTrue(exists);

		revisionId = scenario.delete_A();

		command = new NodeExistsCommandMongo(mongoConnection, "/a", revisionId);
		exists = command.execute();
		assertFalse(exists);

		command = new NodeExistsCommandMongo(mongoConnection, "/a/b",
				revisionId);
		exists = command.execute();
		assertFalse(exists);
	}

	@Test
	public void withoutRevisionId() throws Exception {
		SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
		scenario.create();

		NodeExistsCommandMongo command = new NodeExistsCommandMongo(
				mongoConnection, "/a", null /* revisionId */);
		boolean exists = command.execute();
		assertTrue(exists);

		scenario.delete_A();

		command = new NodeExistsCommandMongo(mongoConnection, "/a", null /* revisionId */);
		exists = command.execute();
		assertFalse(exists);
	}

	@Test
	public void withInvalidRevisionId() throws Exception {
		SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
		scenario.create();

		NodeExistsCommandMongo command = new NodeExistsCommandMongo(
				mongoConnection, "/a", "123456789");
		try {
			command.execute();
			fail("Expected: Invalid revision id exception");
		} catch (Exception expected) {
		}
	}

	@Test
	public void parentDelete() throws Exception {
		SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
		scenario.create();

		NodeExistsCommandMongo command = new NodeExistsCommandMongo(
				mongoConnection, "/a/b", null);
		boolean exists = command.execute();
		assertTrue(exists);

		scenario.delete_A();
		command = new NodeExistsCommandMongo(mongoConnection, "/a/b", null);
		exists = command.execute();
		assertFalse(exists);
	}

	@Test
	public void grandParentDelete() throws Exception {
		// Add a->b->c->d.
		List<Instruction> instructions = new LinkedList<Instruction>();
		instructions.add(new AddNodeInstructionImpl("/", "a"));
		instructions.add(new AddNodeInstructionImpl("/a", "b"));
		instructions.add(new AddNodeInstructionImpl("/a/b", "c"));
		instructions.add(new AddNodeInstructionImpl("/a/b/c", "d"));

		Commit commit = new CommitImpl("Add nodes", "/", "TODO", instructions);
		CommitCommandMongo command = new CommitCommandMongo(mongoConnection,
				commit);
		command.execute();

		// Remove b.
		instructions = new LinkedList<Instruction>();
		instructions.add(new RemoveNodeInstructionImpl("/a", "b"));
		commit = new CommitImpl("Delete /b", "/a", "-b", instructions);
		command = new CommitCommandMongo(mongoConnection, commit);
		command.execute();

		// Check for d.
		NodeExistsCommandMongo existsCommand = new NodeExistsCommandMongo(
				mongoConnection, "/a/b/c/d", null);
		boolean exists = existsCommand.execute();
		assertFalse(exists);
	}

	@Test
	public void existsInOldRevNotInNewRev() throws Exception {
		SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
		String rev1 = scenario.create();
		String rev2 = scenario.delete_A();

		NodeExistsCommandMongo command = new NodeExistsCommandMongo(
				mongoConnection, "/a", rev1);
		boolean exists = command.execute();
		assertTrue(exists);

		command = new NodeExistsCommandMongo(mongoConnection, "/a", rev2);
		exists = command.execute();
		assertFalse(exists);
	}

	@Test
	public void siblingDelete() throws Exception {
		SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
		scenario.create();

		scenario.delete_B();
		NodeExistsCommandMongo command = new NodeExistsCommandMongo(
				mongoConnection, "/a/b", null);
		boolean exists = command.execute();
		assertFalse(exists);

		command = new NodeExistsCommandMongo(mongoConnection, "/a/c", null);
		exists = command.execute();
		assertTrue(exists);
	}

	@Test
	public void testNodeNotFound() throws Exception {

		// adds nodes /a,/a/b,/a/b/c , checks if node a exists
		List<Instruction> instructions = new LinkedList<Instruction>();

		// commit node /a
		instructions.add(new AddNodeInstructionImpl("/", "a"));
		Commit commit1 = new CommitImpl("/", "+a : {}", "Add node a",
				instructions);
		CommitCommandMongo command = new CommitCommandMongo(mongoConnection,
				commit1);
		command.execute();

		// commit node /a/b
		instructions = new LinkedList<Instruction>();
		instructions.add(new AddNodeInstructionImpl("/a", "b"));
		Commit commit2 = new CommitImpl("/a", "+b : {}", "Add node a/b",
				instructions);
		command = new CommitCommandMongo(mongoConnection, commit2);
		command.execute();

		// commit node /a/b/c
		instructions = new LinkedList<Instruction>();
		instructions.add(new AddNodeInstructionImpl("/a/b", "c"));
		Commit commit3 = new CommitImpl("a/b", "+c : {}", "Add node a/b/c",
				instructions);
		command = new CommitCommandMongo(mongoConnection, commit3);
		command.execute();

		// verify if node a is visible in the head revision
		NodeExistsCommandMongo isNodeVisible = new NodeExistsCommandMongo(
				mongoConnection, "/a", null);
		boolean exists = isNodeVisible.execute();
		assertTrue("The node a is not found in the head revision!", exists);

	}
	
	@Test
	public void testTreeDepth() throws Exception {

		String path = "/";
		List<Instruction> instructions = new LinkedList<Instruction>();

		for (int i = 0; i < 1000; i++) {
			instructions.clear();
			instructions.add(new AddNodeInstructionImpl(path, "N" + i));
			Commit commit1 = new CommitImpl(path, "+N" + i + " : {}",
					"Add node N" + i, instructions);
			CommitCommandMongo command = new CommitCommandMongo(
					mongoConnection, commit1);
			command.execute();
			path = (path.endsWith("/")) ? (path = path + "N" + i)
					: (path = path + "/N" + i);
			//System.out.println("*********" + path.length() + "*****");
		}
		
	}

}
