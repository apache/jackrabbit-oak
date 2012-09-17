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

import java.io.InputStream;
import java.util.Properties;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.NodeStoreMongo;
import org.apache.jackrabbit.mongomk.api.BlobStore;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.perf.BlobStoreFS;
import org.apache.jackrabbit.mongomk.perf.Config;




public class MultipleNodesTestBase {

	
	protected static MongoConnection mongoConnection;
	private static Config config;

	static void initMongo() throws Exception {
		mongoConnection = new MongoConnection(config.getMongoHost(),
				config.getMongoPort(), config.getMongoDatabase());
	}

	static MongoMicroKernel initMicroKernel() throws Exception {
		NodeStore nodeStore = new NodeStoreMongo(mongoConnection);
		BlobStore blobStore = new BlobStoreFS(
				System.getProperty("java.io.tmpdir"));
		return new MongoMicroKernel(nodeStore, blobStore);
	}

	static void readConfig() throws Exception {
		InputStream is = MultipleNodesTestBase.class
				.getResourceAsStream("/config.cfg");
		Properties properties = new Properties();
		properties.load(is);
		is.close();
		config = new Config(properties);
	}
}
