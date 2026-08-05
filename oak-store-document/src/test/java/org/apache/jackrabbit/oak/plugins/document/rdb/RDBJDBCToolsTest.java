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

import java.sql.DatabaseMetaData;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools}.
 */
public class RDBJDBCToolsTest {

    @Test
    public void versionCheckWhenDbVersionBelowMinimumReturnsDiagnostic() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(13);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);

        String result = RDBJDBCTools.versionCheck(md, 14, 0, "PostgreSQL");

        Assert.assertTrue(result.contains("Unsupported PostgreSQL version"));
        Assert.assertTrue(result.contains("expected at least 14.0"));
    }

    @Test
    public void versionCheckWhenDbVersionAtMinimumReturnsEmpty() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(14);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);

        String result = RDBJDBCTools.versionCheck(md, 14, 0, "PostgreSQL");

        Assert.assertEquals("", result);
    }

    @Test
    public void versionCheckWhenDbVersionAboveMinimumReturnsEmpty() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(15);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(2);

        String result = RDBJDBCTools.versionCheck(md, 14, 0, "PostgreSQL");

        Assert.assertEquals("", result);
    }

    @Test
    public void versionCheckWhenDriverVersionBelowMinimumReturnsDiagnostic() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(14);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);
        Mockito.when(md.getDriverMajorVersion()).thenReturn(42);
        Mockito.when(md.getDriverMinorVersion()).thenReturn(6);
        Mockito.when(md.getDriverName()).thenReturn("PostgreSQL JDBC Driver");

        String result = RDBJDBCTools.versionCheck(md, 14, 0, 42, 7, "PostgreSQL");

        Assert.assertTrue(result.contains("Unsupported PostgreSQL driver version"));
        Assert.assertTrue(result.contains("expected at least 42.7"));
    }

    @Test
    public void postgresCheckVersionRejectsVersionBelowFourteen() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(13);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);
        Mockito.when(md.getDriverMajorVersion()).thenReturn(42);
        Mockito.when(md.getDriverMinorVersion()).thenReturn(7);

        String result = RDBDocumentStoreDB.POSTGRES.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 14.0"));
    }

    @Test
    public void postgresCheckVersionAcceptsVersionFourteen() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(14);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);
        Mockito.when(md.getDriverMajorVersion()).thenReturn(42);
        Mockito.when(md.getDriverMinorVersion()).thenReturn(7);

        String result = RDBDocumentStoreDB.POSTGRES.checkVersion(md);

        Assert.assertEquals("", result);
    }
}
