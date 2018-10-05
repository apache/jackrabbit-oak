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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
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
    public void init0then2() {
        RDBOptions op = new RDBOptions().tablePrefix("T0T2").initialSchema(0).upgradeToSchema(0).dropTablesOnClose(true);
        RDBDocumentStore rdb0 = null;
        RDBDocumentStore rdb1 = null;
        try {
            rdb0 = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta0 = rdb0.getTable(Collection.NODES);
            assertFalse(meta0.hasVersion());
            rdb1 = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), new RDBOptions().tablePrefix("T0T2").initialSchema(0).upgradeToSchema(2));
            RDBTableMetaData meta1 = rdb1.getTable(Collection.NODES);
            assertTrue(meta1.hasVersion());
            UpdateOp testInsert = new UpdateOp(Utils.getIdFromPath("/foo"), true);
            testInsert.set(NodeDocument.SD_TYPE, 123L);
            assertTrue(rdb1.create(Collection.NODES, Collections.singletonList(testInsert)));
            // check that old instance can read a new entry
            NodeDocument check = rdb0.find(Collection.NODES, Utils.getIdFromPath("/foo"));
            assertNotNull(check);
            assertEquals(123L, check.get(NodeDocument.SD_TYPE));
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
    public void init12() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(RDBDocumentStore.class.getName()).enable(Level.INFO)
                .contains("to DB level 2").create();
        logCustomizer.starting();

        RDBOptions op = new RDBOptions().tablePrefix("T12").initialSchema(1).upgradeToSchema(2).dropTablesOnClose(true);
        RDBDocumentStore rdb = null;
        try {
            rdb = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta = rdb.getTable(Collection.NODES);
            assertEquals(op.getTablePrefix() + "_NODES", meta.getName());
            assertTrue(meta.hasSplitDocs());
            int statementsPerTable = 5;
            assertEquals("unexpected # of log entries: " + logCustomizer.getLogs(),
                    statementsPerTable * RDBDocumentStore.getTableNames().size(), logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
            if (rdb != null) {
                rdb.dispose();
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

    @Test
    public void init22() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(RDBDocumentStore.class.getName()).enable(Level.INFO)
                .contains("to DB level").create();
        logCustomizer.starting();

        RDBOptions op = new RDBOptions().tablePrefix("T" + "22").initialSchema(2).upgradeToSchema(2).dropTablesOnClose(true);
        RDBDocumentStore rdb = null;
        try {
            rdb = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta = rdb.getTable(Collection.NODES);
            assertEquals(op.getTablePrefix() + "_NODES", meta.getName());
            assertTrue(meta.hasVersion());
            assertTrue(meta.hasSplitDocs());
            assertEquals("unexpected # of log entries: " + logCustomizer.getLogs(), 0, logCustomizer.getLogs().size());
        } finally {
            logCustomizer.finished();
            if (rdb != null) {
                rdb.dispose();
            }
        }
    }

    @Test
    public void testVersionGCOnOldDB() {
        RDBOptions op = new RDBOptions().tablePrefix("T11").initialSchema(1).upgradeToSchema(1).dropTablesOnClose(true);
        RDBDocumentStore rdb = null;
        Iterable<NodeDocument> garbage = null;
        try {
            rdb = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), op);
            RDBTableMetaData meta = rdb.getTable(Collection.NODES);
            assertEquals(op.getTablePrefix() + "_NODES", meta.getName());
            assertTrue(meta.hasVersion());
            RDBVersionGCSupport vgc = new RDBVersionGCSupport(rdb);
            Set<NodeDocument.SplitDocType> gctypes = EnumSet.of(SplitDocType.DEFAULT_LEAF, SplitDocType.COMMIT_ROOT_ONLY,
                    SplitDocType.DEFAULT_NO_BRANCH);
            garbage = vgc.identifyGarbage(gctypes, new RevisionVector(), 0L);
            int cnt = 0;
            for (NodeDocument g : garbage) {
                // get rid of compiler warning about g not being used
                if (g.getId() != null) {
                    cnt++;
                }
            }
            assertTrue(cnt == 0);
        } finally {
            if (garbage != null) {
                Utils.closeIfCloseable(garbage);
            }
            if (rdb != null) {
                rdb.dispose();
            }
        }
    }

    @Test
    public void testVersionGCOnMixedModeDB() {
        long sdmaxrev = 1L;
        RDBDocumentStore rdb0 = null;
        RDBDocumentStore rdb1 = null;
        RDBDocumentStore rdb2 = null;
        Iterable<NodeDocument> garbage = null;
        Set<String> expected = new HashSet<String>();
        try {
            // create schema-0 ds and write one split document and one regular document
            {
                RDBOptions options = new RDBOptions().tablePrefix("TMIXED").initialSchema(0).upgradeToSchema(0).dropTablesOnClose(true);
                rdb0 = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), options);
                RDBTableMetaData meta = rdb0.getTable(Collection.NODES);
                assertFalse(meta.hasVersion());
                assertFalse(meta.hasSplitDocs());

                ArrayList<UpdateOp> ops = new ArrayList<UpdateOp>();

                UpdateOp op01 = new UpdateOp("1:p/a", true);
                op01.set(NodeDocument.SD_TYPE, SplitDocType.DEFAULT_LEAF.typeCode());
                op01.set(NodeDocument.SD_MAX_REV_TIME_IN_SECS, sdmaxrev);

                UpdateOp op02 = new UpdateOp(Utils.getIdFromPath("/regular"), true);

                ops.add(op01);
                ops.add(op02);

                assertTrue(rdb0.create(Collection.NODES, ops));
                expected.add(op01.getId());
            }

            // upgrade to schema 1 and write one split document
            {
                RDBOptions options = new RDBOptions().tablePrefix("TMIXED").initialSchema(0).upgradeToSchema(1).dropTablesOnClose(false);
                rdb1 = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), options);
                RDBTableMetaData meta = rdb1.getTable(Collection.NODES);
                assertTrue(meta.hasVersion());
                assertFalse(meta.hasSplitDocs());

                UpdateOp op1 = new UpdateOp("1:p/b", true);
                op1.set(NodeDocument.SD_TYPE, SplitDocType.DEFAULT_LEAF.typeCode());
                op1.set(NodeDocument.SD_MAX_REV_TIME_IN_SECS, sdmaxrev);

                assertTrue(rdb1.create(Collection.NODES, Collections.singletonList(op1)));
                expected.add(op1.getId());
            }

            // upgrade to schema 2, add another split document
            {
                RDBOptions options2 = new RDBOptions().tablePrefix("TMIXED").initialSchema(0).upgradeToSchema(2).dropTablesOnClose(false);
                rdb2 = new RDBDocumentStore(this.ds, new DocumentMK.Builder(), options2);
                RDBTableMetaData meta2 = rdb2.getTable(Collection.NODES);
                assertTrue(meta2.hasVersion());
                assertTrue(meta2.hasSplitDocs());

                UpdateOp op2 = new UpdateOp("1:p/c", true);
                op2.set(NodeDocument.SD_TYPE, SplitDocType.COMMIT_ROOT_ONLY.typeCode());
                op2.set(NodeDocument.SD_MAX_REV_TIME_IN_SECS, sdmaxrev);

                assertTrue(rdb2.create(Collection.NODES, Collections.singletonList(op2)));
                expected.add(op2.getId());
            }

            // GC should find both
            RDBVersionGCSupport vgc = new RDBVersionGCSupport(rdb2);
            Set<NodeDocument.SplitDocType> gctypes = EnumSet.of(SplitDocType.DEFAULT_LEAF, SplitDocType.COMMIT_ROOT_ONLY,
                    SplitDocType.DEFAULT_NO_BRANCH);
            garbage = vgc.identifyGarbage(gctypes, new RevisionVector(), sdmaxrev * 1000 + 10000);
            Set<String> found = new HashSet<String>(); 
            for (NodeDocument g : garbage) {
                found.add(g.getId());
            }
            assertEquals(expected, found);

        } finally {
            if (garbage != null) {
                Utils.closeIfCloseable(garbage);
            }
            if (rdb2 != null) {
                rdb2.dispose();
            }
            if (rdb1 != null) {
                rdb1.dispose();
            }
            if (rdb0 != null) {
                rdb0.dispose();
            }
        }
    }
}
