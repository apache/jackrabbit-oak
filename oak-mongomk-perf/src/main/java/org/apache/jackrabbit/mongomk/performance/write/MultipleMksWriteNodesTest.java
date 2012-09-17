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

import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Writing tests with multiple Mks.
 * @author rogoz
 *
 */
public class MultipleMksWriteNodesTest extends MultipleNodesTestBase {

	static int mkNumber = 5;
	static long nodesNumber=2000;
	static SimpleWriter[] sWorker = new SimpleWriter[mkNumber];
	static AdvanceWriter[] aWorker = new AdvanceWriter[mkNumber];

	@BeforeClass
	public static void init() throws Exception {
		readConfig();
		initMongo();
		for (int i = 0; i < mkNumber; i++) {
			MongoMicroKernel mk = initMicroKernel();
			sWorker[i] = new SimpleWriter("Thread " + i, mk,nodesNumber);
			aWorker[i] = new AdvanceWriter("Thread " + i, mk,nodesNumber);
		}
	}

	@Before
	public void cleanDatabase() {
		MongoUtil.initDatabase(mongoConnection);
	}

	/**
	 * Each worker creates 2000 nodes on the same level.
	 * 5 workers x 2000 nodes=10000 nodes
	 * @throws InterruptedException
	 */
	@Test
	public void testWriteSameLine() throws InterruptedException {

		for (int i = 0; i < mkNumber; i++) {
			sWorker[i].start();

		}
		for (int i = 0; i < mkNumber; i++) {
			sWorker[i].join();
		}
	}

	/**
	 * Each worker is creating a pyramid containing 2000 nodes.
	 * 5 workers x 2000 nodes=10000 nodes
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void testWritePyramid() throws InterruptedException {

		for (int i = 0; i < mkNumber; i++) {
			aWorker[i].start();

		}
		for (int i = 0; i < mkNumber; i++) {
			aWorker[i].join();
		}
	}

}

class SimpleWriter extends Thread {

	MongoMicroKernel mk;

	long nodesNumber;

	public SimpleWriter(String str, MongoMicroKernel mk, long nodesNumber) {
		super(str);
		this.mk = mk;
		this.nodesNumber = nodesNumber;
	}

	public void run() {
		for (int i = 0; i < nodesNumber; i++) {
			TestUtil.createNode(mk, "/", getId() + "No" + i);
		}
	}
}

class AdvanceWriter extends Thread {

	MongoMicroKernel mk;
	long nodesNumber;

	public AdvanceWriter(String str, MongoMicroKernel mk, long nodesNumber) {
		super(str);
		this.mk = mk;
		this.nodesNumber = nodesNumber;
	}

	public void run() {

		TestUtil.insertNode(mk, "/", 0, 50, nodesNumber, "T" + getId());

	}
}
