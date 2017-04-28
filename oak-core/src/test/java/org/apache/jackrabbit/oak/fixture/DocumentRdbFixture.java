/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.fixture;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class DocumentRdbFixture extends NodeStoreFixture {

    private final Map<NodeStore, DataSource> dataSources = new ConcurrentHashMap<NodeStore, DataSource>();

    private String jdbcUrl;

    private final String fname = (new File("target")).isDirectory() ? "target/" : "";

    private final String pUrl = System.getProperty("rdb.jdbc-url", "jdbc:h2:file:./{fname}oaktest");

    private final String pUser = System.getProperty("rdb.jdbc-user", "sa");

    private final String pPasswd = System.getProperty("rdb.jdbc-passwd", "");

    @Override
    public NodeStore createNodeStore() {
        String prefix = "T" + Long.toHexString(System.currentTimeMillis());
        RDBOptions options = new RDBOptions().tablePrefix(prefix).dropTablesOnClose(true);
        this.jdbcUrl = pUrl.replace("{fname}", fname);
        DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcUrl, pUser, pPasswd);

        NodeStore result = new DocumentMK.Builder().setPersistentCache("target/persistentCache,time")
                .setRDBConnection(ds, options).getNodeStore();
        this.dataSources.put(result, ds);
        return result;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        if (nodeStore instanceof DocumentNodeStore) {
            ((DocumentNodeStore) nodeStore).dispose();
        }
        DataSource ds = this.dataSources.remove(nodeStore);
        if (ds instanceof Closeable) {
            try {
                ((Closeable)ds).close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public String toString() {
        return "DocumentNodeStore[RDB] on " + StringUtils.defaultString(this.jdbcUrl, this.pUrl);
    }
}