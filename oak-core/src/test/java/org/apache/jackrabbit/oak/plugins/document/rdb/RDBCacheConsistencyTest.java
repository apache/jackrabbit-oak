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

import java.util.ArrayList;
import java.util.Collection;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.CacheConsistencyTestBase;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RDBCacheConsistencyTest extends CacheConsistencyTestBase {

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        return fixtures(false);
    }

    private final DocumentStoreFixture dsf;

    public RDBCacheConsistencyTest(DocumentStoreFixture dsf) {
        this.dsf = dsf;
    }

    protected static Collection<Object[]> fixtures(boolean multi) {
        Collection<Object[]> result = new ArrayList<Object[]>();
        DocumentStoreFixture candidates[] = new DocumentStoreFixture[] {
                DocumentStoreFixture.RDB_H2, DocumentStoreFixture.RDB_DERBY,
                DocumentStoreFixture.RDB_PG, DocumentStoreFixture.RDB_DB2,
                DocumentStoreFixture.RDB_MYSQL, DocumentStoreFixture.RDB_ORACLE,
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

    @Override
    public DocumentStoreFixture getFixture() {
        return dsf;
    }

    @Override
    public void setTemporaryUpdateException(String msg) {
        DataSource ds = dsf.getRDBDataSource();
        if (ds instanceof RDBDataSourceWrapper) {
            ((RDBDataSourceWrapper) ds).setTemporaryCommitException(msg);
        } else {
            fail();
        }
    }
}
