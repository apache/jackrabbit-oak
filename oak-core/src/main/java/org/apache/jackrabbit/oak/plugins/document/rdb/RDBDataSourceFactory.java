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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.jackrabbit.mk.api.MicroKernelException;

public class RDBDataSourceFactory {

    public static DataSource forJdbcUrl(String url, String username, String passwd) {
        try {
            BasicDataSource bds = new BasicDataSource();
            Driver d = DriverManager.getDriver(url);
            bds.setDriverClassName(d.getClass().getName());
            bds.setUsername(username);
            bds.setPassword(passwd);
            bds.setUrl(url);
            return bds;
        } catch (SQLException ex) {
            throw new MicroKernelException("trying to obtain driver for " + url, ex);
        }
    }
}
