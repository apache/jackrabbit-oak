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
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStoreDB}.
 */
public class RDBBlobStoreDBTest {

    @Test
    public void h2CheckVersionRejectsVersionBelowMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(1);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(3);

        String result = RDBBlobStoreDB.H2.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 1.4"));
    }

    @Test
    public void h2CheckVersionAcceptsVersionAtMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(1);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(4);

        String result = RDBBlobStoreDB.H2.checkVersion(md);

        Assert.assertEquals("", result);
    }

    @Test
    public void derbyCheckVersionRejectsVersionBelowMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(10);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(10);

        String result = RDBBlobStoreDB.DERBY.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 10.11"));
    }

    @Test
    public void db2CheckVersionRejectsVersionBelowMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(9);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(7);

        String result = RDBBlobStoreDB.DB2.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 10.1"));
    }

    @Test
    public void db2GetDataTableCreationStatementUsesBlobType() {
        String statement = RDBBlobStoreDB.DB2.getDataTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("MYTABLE"));
        Assert.assertTrue(statement.contains("blob("));
    }

    @Test
    public void mssqlCheckVersionRejectsVersionBelowMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(10);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);

        String result = RDBBlobStoreDB.MSSQL.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 11.0"));
    }

    @Test
    public void mssqlGetDataTableCreationStatementUsesVarbinaryType() {
        String statement = RDBBlobStoreDB.MSSQL.getDataTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("varbinary(max)"));
        Assert.assertTrue(statement.contains("MYTABLE_PK"));
    }

    @Test
    public void mssqlGetMetaTableCreationStatementNamesPrimaryKey() {
        String statement = RDBBlobStoreDB.MSSQL.getMetaTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("MYTABLE_PK"));
    }

    @Test
    public void mssqlEvaluateDiagnosticsWarnsOnSqlCollation() {
        Map<String, String> diags = new HashMap<>();
        diags.put("collation_name", "SQL_Latin1_General_CP1_CI_AS");

        String result = RDBBlobStoreDB.MSSQL.evaluateDiagnostics(diags);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("OAK-8908"));
    }

    @Test
    public void mssqlEvaluateDiagnosticsReturnsNullForNonSqlCollation() {
        Map<String, String> diags = new HashMap<>();
        diags.put("collation_name", "Latin1_General_CI_AS");

        String result = RDBBlobStoreDB.MSSQL.evaluateDiagnostics(diags);

        Assert.assertNull(result);
    }

    @Test
    public void mssqlEvaluateDiagnosticsReturnsNullWhenCollationMissing() {
        String result = RDBBlobStoreDB.MSSQL.evaluateDiagnostics(new HashMap<>());

        Assert.assertNull(result);
    }

    @Test
    public void mysqlCheckVersionRejectsVersionBelowMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(5);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);

        String result = RDBBlobStoreDB.MYSQL.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 5.5"));
    }

    @Test
    public void mysqlGetDataTableCreationStatementUsesMediumblobType() {
        String statement = RDBBlobStoreDB.MYSQL.getDataTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("mediumblob"));
    }

    @Test
    public void oracleCheckVersionRejectsVersionBelowMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(11);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(2);
        Mockito.when(md.getDriverMajorVersion()).thenReturn(12);
        Mockito.when(md.getDriverMinorVersion()).thenReturn(1);

        String result = RDBBlobStoreDB.ORACLE.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 12.1"));
    }

    @Test
    public void oracleGetMetaTableCreationStatementUsesNumberType() {
        String statement = RDBBlobStoreDB.ORACLE.getMetaTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("LVL number"));
        Assert.assertTrue(statement.contains("LASTMOD number"));
    }

    @Test
    public void postgresCheckVersionRejectsVersionBelowMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(13);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);

        String result = RDBBlobStoreDB.POSTGRES.checkVersion(md);

        Assert.assertTrue(result.contains("expected at least 14.0"));
    }

    @Test
    public void postgresCheckVersionAcceptsVersionAtMinimum() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseMajorVersion()).thenReturn(14);
        Mockito.when(md.getDatabaseMinorVersion()).thenReturn(0);
        Mockito.when(md.getDriverMajorVersion()).thenReturn(9);
        Mockito.when(md.getDriverMinorVersion()).thenReturn(4);

        String result = RDBBlobStoreDB.POSTGRES.checkVersion(md);

        Assert.assertEquals("", result);
    }

    @Test
    public void postgresGetDataTableCreationStatementUsesByteaType() {
        String statement = RDBBlobStoreDB.POSTGRES.getDataTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("bytea"));
    }

    @Test
    public void defaultCheckVersionReturnsUnknownDatabaseMessage() throws Exception {
        DatabaseMetaData md = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(md.getDatabaseProductName()).thenReturn("SomeUnknownDB");

        String result = RDBBlobStoreDB.DEFAULT.checkVersion(md);

        Assert.assertTrue(result.contains("Unknown database type: SomeUnknownDB"));
    }

    @Test
    public void defaultGetDataTableCreationStatementUsesBlobType() {
        String statement = RDBBlobStoreDB.DEFAULT.getDataTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("DATA blob)"));
    }

    @Test
    public void defaultGetMetaTableCreationStatementUsesDefaultTypes() {
        String statement = RDBBlobStoreDB.DEFAULT.getMetaTableCreationStatement("MYTABLE");

        Assert.assertTrue(statement.contains("LVL int"));
        Assert.assertTrue(statement.contains("LASTMOD bigint"));
    }

    @Test
    public void evaluateDiagnosticsDefaultsToNull() {
        Assert.assertNull(RDBBlobStoreDB.H2.evaluateDiagnostics(new HashMap<>()));
        Assert.assertNull(RDBBlobStoreDB.POSTGRES.evaluateDiagnostics(new HashMap<>()));
    }

    @Test
    public void toStringReturnsDescription() {
        Assert.assertEquals("PostgreSQL", RDBBlobStoreDB.POSTGRES.toString());
        Assert.assertEquals("Microsoft SQL Server", RDBBlobStoreDB.MSSQL.toString());
    }

    @Test
    public void getValueMatchesKnownDescription() {
        Assert.assertEquals(RDBBlobStoreDB.POSTGRES, RDBBlobStoreDB.getValue("PostgreSQL"));
        Assert.assertEquals(RDBBlobStoreDB.MYSQL, RDBBlobStoreDB.getValue("MySQL"));
    }

    @Test
    public void getValueMatchesDb2WithSchemaSuffix() {
        Assert.assertEquals(RDBBlobStoreDB.DB2, RDBBlobStoreDB.getValue("DB2/LINUXX8664"));
    }

    @Test
    public void getValueFallsBackToDefaultForUnknownDescription() {
        RDBBlobStoreDB result = RDBBlobStoreDB.getValue("SomeUnknownDB");

        Assert.assertEquals(RDBBlobStoreDB.DEFAULT, result);
        Assert.assertTrue(result.toString().contains("SomeUnknownDB"));
        Assert.assertTrue(result.toString().contains("using default settings"));
    }

    @Test
    public void getAdditionalDiagnosticsDelegatesToVendorCode() {
        RDBConnectionHandler ch = Mockito.mock(RDBConnectionHandler.class);

        Map<String, String> result = RDBBlobStoreDB.H2.getAdditionalDiagnostics(ch, "MYTABLE");

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }
}
