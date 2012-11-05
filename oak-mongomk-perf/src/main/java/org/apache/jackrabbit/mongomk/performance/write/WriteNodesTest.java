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
package org.apache.jackrabbit.mongomk.performance.write;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Measures the time needed for creating different tree node structures.Only one
 * mongoMk is used for writing operation.
 */
public class WriteNodesTest extends MultipleNodesTestBase {
	static MicroKernel mk;

	@BeforeClass
	public static void init() throws Exception {
		readConfig();
		initMongo();
		mk=initMicroKernel();
	}

	@Before
	public void cleanDatabase() {
	    mongoConnection.initializeDB(true);
	}

	/**
	 * Creates 10000 nodes, all with on the same level with the same parent
	 * node.
	 */
	@Test
	public void addNodesInLine() {
		int nodesNumber = 10000;
		TestUtil.insertNode(mk, "/", 0, 0, nodesNumber, "N");
	}

	/**
	 * Creates 10000 nodes, all of them having 10 children nodes.
	 */
	@Test
	public void addNodes10Children() {
		int nodesNumber = 10000;
		TestUtil.insertNode(mk, "/", 0, 10, nodesNumber, "N");
	}

	/**
	 * Creates 10000 nodes, all of them having 100 children nodes.
	 */
	@Test
	public void addNodes100Children() {
		int nodesNumber = 10000;
		TestUtil.insertNode(mk, "/", 0, 100, nodesNumber, "N");
	}

	/**
	 * Creates 10000 nodes, all of them on different levels.Each node has one
	 * child only.
	 */
	@Test
	public void addNodes1Child() {
		int nodesNumber = 2000;
		TestUtil.insertNode(mk, "/", 0, 1, nodesNumber,"N");
	}

}
