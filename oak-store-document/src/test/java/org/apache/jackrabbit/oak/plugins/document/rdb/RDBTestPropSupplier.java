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

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;

/**
 * System property suppliers for RDB tests.
 */
public class RDBTestPropSupplier {

    public SystemPropertySupplier<String> url, username, passwd;

    public RDBTestPropSupplier(SystemPropertySupplier<String> url, SystemPropertySupplier<String> username,
            SystemPropertySupplier<String> passwd) {
        this.url = url;
        this.username = username;
        this.passwd = passwd.hideValue();
    }

    private static String url(String db) {
        return "rdb-" + db + "-jdbc-url";
    }

    private static String username(String db) {
        return "rdb-" + db + "-jdbc-user";
    }

    private static String passwd(String db) {
        return "rdb-" + db + "-jdbc-passwd";
    }

    private static final SystemPropertySupplier<String> RDBDB2URL = SystemPropertySupplier.create(url("db2"),
            "jdbc:db2://localhost:50000/OAK");
    private static final SystemPropertySupplier<String> RDBDB2USER = SystemPropertySupplier.create(username("db2"), "oak");
    private static final SystemPropertySupplier<String> RDBDB2PASSWD = SystemPropertySupplier.create(passwd("db2"), "geheim");

    public static final RDBTestPropSupplier DB2 = new RDBTestPropSupplier(RDBDB2URL, RDBDB2USER, RDBDB2PASSWD);

    private static final SystemPropertySupplier<String> RDBDERBYURL = SystemPropertySupplier.create(url("derby"),
            "jdbc:derby:./target/derby-ds-test;create=true");
    private static final SystemPropertySupplier<String> RDBDERBYUSER = SystemPropertySupplier.create(username("derby"), "sa");
    private static final SystemPropertySupplier<String> RDBDERBYPASSWD = SystemPropertySupplier.create(passwd("derby"), "");

    public static final RDBTestPropSupplier DERBY = new RDBTestPropSupplier(RDBDERBYURL, RDBDERBYUSER, RDBDERBYPASSWD);

    private static final SystemPropertySupplier<String> RDBH2URL = SystemPropertySupplier.create(url("h2"),
            "jdbc:h2:file:./target/h2-ds-test");
    private static final SystemPropertySupplier<String> RDBH2USER = SystemPropertySupplier.create(username("h2"), "sa");
    private static final SystemPropertySupplier<String> RDBH2PASSWD = SystemPropertySupplier.create(passwd("h2"), "");

    public static final RDBTestPropSupplier H2 = new RDBTestPropSupplier(RDBH2URL, RDBH2USER, RDBH2PASSWD);

    private static final SystemPropertySupplier<String> RDBMSSQLURL = SystemPropertySupplier.create(url("mssql"),
            "jdbc:sqlserver://localhost:1433;databaseName=OAK");
    private static final SystemPropertySupplier<String> RDBMSSQLUSER = SystemPropertySupplier.create(username("mssql"), "sa");
    private static final SystemPropertySupplier<String> RDBMSSQLPASSWD = SystemPropertySupplier.create(passwd("mssql"), "geheim");

    public static final RDBTestPropSupplier MSSQL = new RDBTestPropSupplier(RDBMSSQLURL, RDBMSSQLUSER, RDBMSSQLPASSWD);

    private static final SystemPropertySupplier<String> RDBMYSQLURL = SystemPropertySupplier.create(url("mysql"),
            "jdbc:mysql://localhost:3306/oak");
    private static final SystemPropertySupplier<String> RDBMYSQLUSER = SystemPropertySupplier.create(username("mysql"), "root");
    private static final SystemPropertySupplier<String> RDBMYSQLPASSWD = SystemPropertySupplier.create(passwd("mysql"), "geheim");

    public static final RDBTestPropSupplier MYSQL = new RDBTestPropSupplier(RDBMYSQLURL, RDBMYSQLUSER, RDBMYSQLPASSWD);

    private static final SystemPropertySupplier<String> RDBORACLEURL = SystemPropertySupplier.create(url("oracle"),
            "jdbc:oracle:thin:@localhost:1521:orcl");
    private static final SystemPropertySupplier<String> RDBORACLEUSER = SystemPropertySupplier.create(username("oracle"), "system");
    private static final SystemPropertySupplier<String> RDBORACLEPASSWD = SystemPropertySupplier.create(passwd("oracle"), "geheim");

    public static final RDBTestPropSupplier ORACLE = new RDBTestPropSupplier(RDBORACLEURL, RDBORACLEUSER, RDBORACLEPASSWD);

    private static final SystemPropertySupplier<String> RDBPOSTGRESURL = SystemPropertySupplier.create(url("postgres"),
            "jdbc:postgresql:oak");
    private static final SystemPropertySupplier<String> RDBPOSTGRESUSER = SystemPropertySupplier.create(username("postgres"),
            "postgres");
    private static final SystemPropertySupplier<String> RDBPOSTGRESPASSWD = SystemPropertySupplier.create(passwd("postgres"),
            "geheim");

    public static final RDBTestPropSupplier POSTGRES = new RDBTestPropSupplier(RDBPOSTGRESURL, RDBPOSTGRESUSER, RDBPOSTGRESPASSWD);
}
