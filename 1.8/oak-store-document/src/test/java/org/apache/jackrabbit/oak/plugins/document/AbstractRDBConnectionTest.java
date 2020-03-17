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

import java.io.Closeable;
import java.io.File;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Base class for test cases that need a {@link DataSource}
 * to a clean test database. Tests in subclasses are automatically
 * skipped if the configured database connection can not be created.
 */
public class AbstractRDBConnectionTest extends DocumentMKTestBase {

    protected DataSource dataSource;
    protected DocumentMK mk;

    private static final String fname = (new File("target")).isDirectory() ? "target/" : "";
    private static final String RAWURL = System.getProperty("rdb.jdbc-url", "jdbc:h2:file:./target/h2test");
    protected static final String USERNAME = System.getProperty("rdb.jdbc-user", "");
    protected static final String PASSWD = System.getProperty("rdb.jdbc-passwd", "");
    protected static final String URL = RAWURL.replace("{fname}", fname);

    @BeforeClass
    public static void checkRDBAvailable() {
    }

    @Before
    public void setUpConnection() throws Exception {
        dataSource = RDBDataSourceFactory.forJdbcUrl(URL, USERNAME, PASSWD);
        Revision.setClock(getTestClock());
        mk = newBuilder(dataSource).open();
    }

    protected DocumentMK.Builder newBuilder(DataSource db) throws Exception {
        String prefix = "T" + Long.toHexString(System.currentTimeMillis());
        RDBOptions opt = new RDBOptions().tablePrefix(prefix).dropTablesOnClose(true);
        return new DocumentMK.Builder().clock(getTestClock()).setRDBConnection(dataSource, opt);
    }

    protected Clock getTestClock() throws InterruptedException {
        return Clock.SIMPLE;
    }

    @After
    public void tearDownConnection() throws Exception {
        if (mk != null) {
            mk.dispose();
        }
        if (dataSource instanceof Closeable) {
            ((Closeable)dataSource).close();
        }
        Revision.resetClockToDefault();
    }

    @Override
    protected DocumentMK getDocumentMK() {
        return mk;
    }

    protected static byte[] readFully(DocumentMK mk, String blobId) {
        int remaining = (int) mk.getLength(blobId);
        byte[] bytes = new byte[remaining];

        int offset = 0;
        while (remaining > 0) {
            int count = mk.read(blobId, offset, bytes, offset, remaining);
            if (count < 0) {
                break;
            }
            offset += count;
            remaining -= count;
        }
        return bytes;
    }
}
