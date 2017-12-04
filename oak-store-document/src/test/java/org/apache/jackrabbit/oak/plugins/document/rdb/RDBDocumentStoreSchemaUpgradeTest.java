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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.RDBTableMetaData;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ch.qos.logback.classic.Level;

@RunWith(Parameterized.class)
public class RDBDocumentStoreSchemaUpgradeTest {

    @Parameterized.Parameters(name = "{0}")
    public static java.util.Collection<Object[]> fixtures() {
        java.util.Collection<Object[]> result = new ArrayList<Object[]>();
        DocumentStoreFixture candidates[] = new DocumentStoreFixture[] { DocumentStoreFixture.RDB_H2,
                DocumentStoreFixture.RDB_DERBY, DocumentStoreFixture.RDB_PG, DocumentStoreFixture.RDB_DB2,
                DocumentStoreFixture.RDB_MYSQL, DocumentStoreFixture.RDB_ORACLE, DocumentStoreFixture.RDB_MSSQL };

        for (DocumentStoreFixture dsf : candidates) {
            if (dsf.isAvailable()) {
                result.add(new Object[] { dsf });
            }
        }

        return result;
    }

    private DataSource ds;

    public RDBDocumentStoreSchemaUpgradeTest(DocumentStoreFixture dsf) {
        this.ds = dsf.getRDBDataSource();
    }

    @Test
    public void initDefault() {
        RDBOptions op = new RDBOptions().tablePrefix("T00").initialSchema(0).upgradeToSchema(0).dropTablesOnClose(true);
        RDBDocumentStore rdb = null;
        try {
            rdb = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta = rdb.getTable(Collection.NODES);
            assertEquals(op.getTablePrefix() + "_NODES", meta.getName());
            assertFalse(meta.hasVersion());
        } finally {
            if (rdb != null) {
                rdb.dispose();
            }
        }
    }

    @Test
    public void init01() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(RDBDocumentStore.class.getName()).enable(Level.INFO)
                .contains("to DB level 1").create();
        logCustomizer.starting();

        RDBOptions op = new RDBOptions().tablePrefix("T01").initialSchema(0).upgradeToSchema(1).dropTablesOnClose(true);
        RDBDocumentStore rdb = null;
        try {
            rdb = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta = rdb.getTable(Collection.NODES);
            assertEquals(op.getTablePrefix() + "_NODES", meta.getName());
            assertTrue(meta.hasVersion());
            assertEquals("unexpected # of log entries: " + logCustomizer.getLogs(), RDBDocumentStore.getTableNames().size(),
                    logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
            if (rdb != null) {
                rdb.dispose();
            }
        }
    }

    @Test
    public void init0then1() {
        RDBOptions op = new RDBOptions().tablePrefix("T0T1").initialSchema(0).upgradeToSchema(0).dropTablesOnClose(true);
        RDBDocumentStore rdb0 = null;
        RDBDocumentStore rdb1 = null;
        try {
            rdb0 = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta0 = rdb0.getTable(Collection.NODES);
            assertFalse(meta0.hasVersion());
            rdb1 = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), new RDBOptions().tablePrefix("T0T1").initialSchema(0).upgradeToSchema(1));
            RDBTableMetaData meta1 = rdb1.getTable(Collection.NODES);
            assertTrue(meta1.hasVersion());
            UpdateOp testInsert = new UpdateOp(Utils.getIdFromPath("/foo"), true);
            assertTrue(rdb1.create(Collection.NODES, Collections.singletonList(testInsert)));
        } finally {
            if (rdb1 != null) {
                rdb1.dispose();
            }
            if (rdb0 != null) {
                rdb0.dispose();
            }
        }
    }

    @Test
    public void init01fail() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(RDBDocumentStore.class.getName()).enable(Level.INFO)
                .contains("Attempted to upgrade").create();
        logCustomizer.starting();

        Assume.assumeTrue(ds instanceof RDBDataSourceWrapper);
        RDBDataSourceWrapper wds = (RDBDataSourceWrapper)ds;
        wds.setFailAlterTableAddColumnStatements(true);

        RDBOptions op = new RDBOptions().tablePrefix("T01F").initialSchema(0).upgradeToSchema(1).dropTablesOnClose(true);
        RDBDocumentStore rdb = null;
        try {
            rdb = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta = rdb.getTable(Collection.NODES);
            assertEquals(op.getTablePrefix() + "_NODES", meta.getName());
            assertFalse(meta.hasVersion());
            assertEquals("unexpected # of log entries: " + logCustomizer.getLogs(), RDBDocumentStore.getTableNames().size(),
                    logCustomizer.getLogs().size());
            UpdateOp testInsert = new UpdateOp(Utils.getIdFromPath("/foo"), true);
            assertTrue(rdb.create(Collection.NODES, Collections.singletonList(testInsert)));
        } finally {
            wds.setFailAlterTableAddColumnStatements(false);
            logCustomizer.finished();
            if (rdb != null) {
                rdb.dispose();
            }
        }
    }

    @Test
    public void init11() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(RDBDocumentStore.class.getName()).enable(Level.INFO)
                .contains("to DB level 1").create();
        logCustomizer.starting();

        RDBOptions op = new RDBOptions().tablePrefix("T11").initialSchema(1).upgradeToSchema(1).dropTablesOnClose(true);
        RDBDocumentStore rdb = null;
        try {
            rdb = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta = rdb.getTable(Collection.NODES);
            assertEquals(op.getTablePrefix() + "_NODES", meta.getName());
            assertTrue(meta.hasVersion());
            assertEquals("unexpected # of log entries: " + logCustomizer.getLogs(), 0, logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
            if (rdb != null) {
                rdb.dispose();
            }
        }
    }
}
