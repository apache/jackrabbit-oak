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
package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;

/**
 * OFFLINE utility to delete the clusterId stored as hidden
 * property as defined by ClusterRepositoryInfo.
 * <p>
 * This utility is meant to be the only mechanism to delete
 * this id and yes, it is meant to be used offline only
 * (as otherwise this would correspond to breaking the
 * requirement that the clusterId be stable and persistent).
 * <p>
 * Target use case for this tool is to avoid duplicate 
 * clusterIds after a repository was cloned.
 */
class ResetClusterIdCommand implements Command {

    private static void deleteClusterId(NodeStore store) {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder clusterConfigNode = builder.getChildNode(
                ClusterRepositoryInfo.CLUSTER_CONFIG_NODE);
        
        if (!clusterConfigNode.exists()) {
            // if it doesn't exist, then there is no way to delete
            System.out.println("clusterId was never set or already deleted.");
            return;
        }
        
        if (!clusterConfigNode.hasProperty(ClusterRepositoryInfo.CLUSTER_ID_PROP)) {
            // the config node exists, but the clusterId not 
            // so again, no way to delete
            System.out.println("clusterId was never set or already deleted.");
            return;
        }
        String oldClusterId = clusterConfigNode.getProperty(ClusterRepositoryInfo.CLUSTER_ID_PROP)
                .getValue(Type.STRING);
        clusterConfigNode.removeProperty(ClusterRepositoryInfo.CLUSTER_ID_PROP);
        try {
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            System.out.println("clusterId deleted successfully. (old id was " + oldClusterId + ")");
        } catch (CommitFailedException e) {
            System.err.println("Failed to delete clusterId due to exception: "+e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSet options = parser.parse(args);

        if (options.nonOptionArguments().isEmpty()) {
            System.out.println("usage: resetclusterid {<path>|<mongo-uri>}");
            System.exit(1);
        }

        String source = options.nonOptionArguments().get(0).toString();

        Closer closer = Closer.create();
        try {
            NodeStore store;
            if (args[0].startsWith(MongoURI.MONGODB_PREFIX)) {
                MongoClientURI uri = new MongoClientURI(source);
                MongoClient client = new MongoClient(uri);
                final DocumentNodeStore dns = newMongoDocumentNodeStoreBuilder()
                        .setMongoDB(client.getDB(uri.getDatabase()))
                        .build();
                closer.register(Utils.asCloseable(dns));
                store = dns;
            } else {
                store = SegmentTarUtils.bootstrapNodeStore(source, closer);
            }
            
            deleteClusterId(store);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    
    }
    
}