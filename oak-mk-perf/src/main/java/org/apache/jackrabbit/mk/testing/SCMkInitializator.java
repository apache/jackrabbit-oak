/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.testing;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.util.BlobStoreFS;
import org.apache.jackrabbit.mk.util.Configuration;
import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.NodeStoreMongo;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.api.BlobStore;

import org.apache.jackrabbit.mongomk.util.MongoUtil;

import com.mongodb.BasicDBObjectBuilder;

/**
 * Initialize a mongodb microkernel.
 * 
 * @author rogoz
 * 
 */
public class SCMkInitializator implements Initializator {

	/**
	 * Create a microkernel.Initialize the db.
	 */
	public MicroKernel init(Configuration conf) throws Exception {

		MicroKernel mk;
		MongoConnection mongoConnection = new MongoConnection(conf.getHost(),
				conf.getMongoPort(), conf.getMongoDatabase());
		MongoConnection adminConnection = new MongoConnection(conf.getHost(),
				conf.getMongoPort(), "admin");

		NodeStore nodeStore = new NodeStoreMongo(mongoConnection);
		BlobStore blobStore = new BlobStoreFS(
				System.getProperty("java.io.tmpdir"));

		// initialize the database
		//temporary workaround.Remove the sleep.
		Thread.sleep(20000);
		MongoUtil.initDatabase(mongoConnection);
		// set the shard key
		adminConnection.getDB()
				.command(
						BasicDBObjectBuilder
								.start("shardCollection", "test.nodes")
								.push("key").add("path", 1).add("revId", 1)
								.pop().get());

		mk = new MongoMicroKernel(nodeStore, blobStore);
		return mk;
	}

	public String getType() {
		return "SharedCloudMicrokernel implementation";
	}

}
