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
package org.apache.jackrabbit.mk.blobs;

import org.h2.jdbcx.JdbcConnectionPool;

import java.sql.Connection;

/**
 * Tests the DbBlobStore implementation.
 */
public class DbBlobStoreTest extends AbstractBlobStoreTest {

    private Connection sentinel;
    private JdbcConnectionPool cp;

    @Override
    public void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        cp = JdbcConnectionPool.create("jdbc:h2:mem:", "", "");
        sentinel = cp.getConnection();
        DbBlobStore blobStore = new DbBlobStore();
        blobStore.setConnectionPool(cp);
        blobStore.setBlockSize(128);
        blobStore.setBlockSizeMin(48);
        this.store = blobStore;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (sentinel != null) {
            sentinel.close();
        }
        cp.dispose();
    }

}
