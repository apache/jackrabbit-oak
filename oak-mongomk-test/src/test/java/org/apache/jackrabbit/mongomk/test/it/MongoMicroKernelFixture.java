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
package org.apache.jackrabbit.mongomk.test.it;

import java.io.InputStream;
import java.util.Properties;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.test.MicroKernelFixture;
import org.apache.jackrabbit.mongomk.api.BlobStore;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.BlobStoreMongo;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.NodeStoreMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.junit.Assert;


/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class MongoMicroKernelFixture implements MicroKernelFixture {

    private static MongoConnection mongoConnection = createMongoConnection();

    private static MongoConnection createMongoConnection() {
        try {
            InputStream is = MongoMicroKernelFixture.class.getResourceAsStream("/config.cfg");
            Properties properties = new Properties();
            properties.load(is);

            String host = properties.getProperty("mongo.host");
            int port = Integer.parseInt(properties.getProperty("mongo.port"));
            String database = properties.getProperty("mongo.db");

            return new MongoConnection(host, port, database);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setUpCluster(MicroKernel[] cluster) {
        try {
            MongoUtil.initDatabase(mongoConnection);
            NodeStore nodeStore = new NodeStoreMongo(mongoConnection);
            BlobStore blobStore = new BlobStoreMongo(mongoConnection);

            MicroKernel mk = new MongoMicroKernel(nodeStore, blobStore);
            for (int i = 0; i < cluster.length; i++) {
                cluster[i] = mk;
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Override
    public void syncMicroKernelCluster(MicroKernel... nodes) {
    }

    @Override
    public void tearDownCluster(MicroKernel[] cluster) {
        try {
            MongoUtil.clearDatabase(mongoConnection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
