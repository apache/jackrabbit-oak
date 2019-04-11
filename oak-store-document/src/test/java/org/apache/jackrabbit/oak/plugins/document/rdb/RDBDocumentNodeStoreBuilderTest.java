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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.junit.Assert.fail;

import java.util.UUID;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.junit.Test;

public class RDBDocumentNodeStoreBuilderTest {

    @Test
    public void testReadOnly() throws Exception {
        // see OAK-8214

        DataSource dataSource = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID(), "", "");

        RDBDocumentNodeStoreBuilder b = RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder();
        b.setRDBConnection(dataSource);
        b.setReadOnlyMode();

        try {
            b.getDocumentStore();
            fail("should not get here");
        } catch (DocumentStoreException expected) {
        }
    }
}
