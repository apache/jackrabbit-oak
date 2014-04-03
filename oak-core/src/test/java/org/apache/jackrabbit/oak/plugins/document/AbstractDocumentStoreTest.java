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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class AbstractDocumentStoreTest {

    protected String dsname;
    protected DocumentStore ds;
    protected Set<String> removeMe = new HashSet<String>();

    public AbstractDocumentStoreTest(DocumentStoreFixture dsf) {
        this.ds = dsf.createDocumentStore();
        this.dsname = dsf.getName();
    }

    @After
    public void cleanUp() {
        for (String id : removeMe) {
            try {
                ds.remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, id);
            } catch (Exception ex) {
                // best effort
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() {
        Collection<Object[]> result = new ArrayList<Object[]>();
        DocumentStoreFixture candidates[] = new DocumentStoreFixture[] { DocumentStoreFixture.MEMORY, DocumentStoreFixture.MONGO,
                DocumentStoreFixture.RDB_H2, DocumentStoreFixture.RDB_PG, DocumentStoreFixture.RDB_DB2 };

        for (DocumentStoreFixture dsf : candidates) {
            if (dsf.isAvailable()) {
                result.add(new Object[] { dsf });
            }
        }

        return result;
    }
}
