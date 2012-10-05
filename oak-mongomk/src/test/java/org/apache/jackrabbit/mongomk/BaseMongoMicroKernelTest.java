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
package org.apache.jackrabbit.mongomk;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.api.BlobStore;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.BlobStoreMongo;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.NodeStoreMongo;
import org.junit.Before;

/**
 * Base class for {@code MongoDB} tests that need the MongoMK.
 */
public class BaseMongoMicroKernelTest extends BaseMongoTest {

    public static MicroKernel mk;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        NodeStore nodeStore = new NodeStoreMongo(mongoConnection);
        BlobStore blobStore = new BlobStoreMongo(mongoConnection);
        mk = new MongoMicroKernel(nodeStore, blobStore);
    }
}