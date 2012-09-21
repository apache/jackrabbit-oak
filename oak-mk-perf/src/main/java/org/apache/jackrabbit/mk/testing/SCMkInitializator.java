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
		
		//make sure that the nodes collection is dropped
		/*
		String nodeCollectionname = mongoConnection.getNodeCollection().getName();
		long start = System.currentTimeMillis();
		long stop=0;
		
		while (mongoConnection.getDB().collectionExists(nodeCollectionname)&&stop-start>10000){
			stop=System.currentTimeMillis();
			Thread.sleep(2000);
			mongoConnection.getDB().getCollection(nodeCollectionname).drop();
		}*/
		//initialize the database
		Thread.sleep(20000);
		MongoUtil.initDatabase(mongoConnection);
		//set the shard key
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
