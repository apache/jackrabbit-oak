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
import java.util.List;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public abstract class AbstractDocumentStoreTest {

    protected String dsname;
    protected DocumentStore ds;
    protected DocumentStoreFixture dsf;
    protected DataSource rdbDataSource;
    protected List<String> removeMe = new ArrayList<String>();

    static final Logger LOG = LoggerFactory.getLogger(AbstractDocumentStoreTest.class);

    public AbstractDocumentStoreTest(DocumentStoreFixture dsf) {
        this.dsf = dsf;
        this.ds = dsf.createDocumentStore(1);
        this.dsname = dsf.getName();
        this.rdbDataSource = dsf.getRDBDataSource();
    }

    @After
    public void cleanUp() throws Exception {
        if (!removeMe.isEmpty()) {
            long start = System.nanoTime();
            try {
                ds.remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, removeMe);
            } catch (Exception ex) {
                // retry one by one
                for (String id : removeMe) {
                    try {
                        ds.remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, id);
                    } catch (Exception ex2) {
                        // best effort
                    }
                }
            }
            if (removeMe.size() > 1) {
                long elapsed = (System.nanoTime() - start) / (1000 * 1000);
                float rate = (((float)removeMe.size()) / (elapsed == 0 ? 1 : elapsed));
                LOG.info(removeMe.size() + " documents removed in " + elapsed + "ms (" + rate + "/ms)");
            }
        }
        ds.dispose();
        dsf.dispose();
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        return fixtures(false);
    }

    protected static Collection<Object[]> fixtures(boolean multi) {
        Collection<Object[]> result = new ArrayList<Object[]>();
        DocumentStoreFixture candidates[] = new DocumentStoreFixture[] { DocumentStoreFixture.MEMORY, DocumentStoreFixture.MONGO,
                DocumentStoreFixture.RDB_H2, DocumentStoreFixture.RDB_DERBY, DocumentStoreFixture.RDB_PG,
                DocumentStoreFixture.RDB_DB2, DocumentStoreFixture.RDB_MYSQL, DocumentStoreFixture.RDB_ORACLE,
                DocumentStoreFixture.RDB_MSSQL };

        for (DocumentStoreFixture dsf : candidates) {
            if (dsf.isAvailable()) {
                if (!multi || dsf.hasSinglePersistence()) {
                    result.add(new Object[] { dsf });
                }
            }
        }

        return result;
    }

    /**
     * Generate a random string of given size, with or without non-ASCII characters.
     */
    public static String generateString(int length, boolean asciiOnly) {
        char[] s = new char[length];
        for (int i = 0; i < length; i++) {
            if (asciiOnly) {
                s[i] = (char) (32 + (int) (95 * Math.random()));
            } else {
                s[i] = (char) (32 + (int) ((0xd7ff - 32) * Math.random()));
            }
        }
        return new String(s);
    }
}
