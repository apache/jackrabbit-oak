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
    protected List<String> removeMeSettings = new ArrayList<String>();
    protected List<String> removeMeJournal = new ArrayList<String>();

    static final Logger LOG = LoggerFactory.getLogger(AbstractDocumentStoreTest.class);

    public AbstractDocumentStoreTest(DocumentStoreFixture dsf) {
        this.dsf = dsf;
        this.ds = dsf.createDocumentStore(1);
        this.dsname = dsf.getName();
        this.rdbDataSource = dsf.getRDBDataSource();
    }

    @After
    public void cleanUp() throws Exception {
        removeTestNodes(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, removeMe);
        removeTestNodes(org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS, removeMeSettings);
        removeTestNodes(org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL, removeMeJournal);
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

    /**
     * Generate a random string of given size, with or without non-ASCII characters.
     */
    public static String generateConstantString(int length) {
        char[] s = new char[length];
        for (int i = 0; i < length; i++) {
            s[i] = (char)('0' + (i % 10));
        }
        return new String(s);
    }

    private <T extends Document> void removeTestNodes(org.apache.jackrabbit.oak.plugins.document.Collection<T> col,
            List<String> ids) {
        if (!ids.isEmpty()) {
            long start = System.nanoTime();
            try {
                ds.remove(col, ids);
            } catch (Exception ex) {
                // retry one by one
                for (String id : ids) {
                    try {
                        ds.remove(col, id);
                    } catch (Exception ex2) {
                        // best effort
                    }
                }
            }
            if (ids.size() > 1) {
                long elapsed = (System.nanoTime() - start) / (1000 * 1000);
                float rate = (((float) ids.size()) / (elapsed == 0 ? 1 : elapsed));
                LOG.info(ids.size() + " documents removed in " + elapsed + "ms (" + rate + "/ms)");
            }
        }
    }
}
