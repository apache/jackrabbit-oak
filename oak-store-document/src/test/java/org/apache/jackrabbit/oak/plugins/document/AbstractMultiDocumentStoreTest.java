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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

public abstract class AbstractMultiDocumentStoreTest extends AbstractDocumentStoreTest {

    protected DocumentStore ds1, ds2;

    public AbstractMultiDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
        this.ds1 = super.ds;
        this.ds2 = dsf.createDocumentStore(2);
    }

    @BeforeClass
    public static void disableClientSession() {
        // Disable the use of client session for this kind of tests.
        // Most of these tests assume causal consistency across multiple
        // DocumentStore instances, which is not the case when the test
        // runs on a replica set and a client session is used.
        System.setProperty("oak.mongo.clientSession", "false");
    }

    @AfterClass
    public static void resetSystemProperty() {
        System.clearProperty("oak.mongo.clientSession");
    }

    @Override
    public void cleanUp() throws Exception {
        super.cleanUp();
        ds2.dispose();
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        return fixtures(true);
    }
}
