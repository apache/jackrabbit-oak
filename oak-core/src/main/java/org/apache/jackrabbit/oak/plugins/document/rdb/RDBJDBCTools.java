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

import java.util.Locale;

public class RDBJDBCTools {

    protected static String jdbctype(String jdbcurl) {
        if (jdbcurl == null) {
            return null;
        } else {
            String t = jdbcurl.toLowerCase(Locale.ENGLISH);
            if (!t.startsWith("jdbc:")) {
                return null;
            } else {
                t = t.substring("jbdc:".length());
                int p = t.indexOf(":");
                if (p <= 0) {
                    return t;
                } else {
                    return t.substring(0, p);
                }
            }
        }
    }

    protected static String driverForDBType(String type) {
        if ("h2".equals(type)) {
            return "org.h2.Driver";
        } else if ("postgresql".equals(type)) {
            return "org.postgresql.Driver";
        } else if ("db2".equals(type)) {
            return "com.ibm.db2.jcc.DB2Driver";
        } else if ("mysql".equals(type)) {
            return "com.mysql.jdbc.Driver";
        } else if ("oracle".equals(type)) {
            return "oracle.jdbc.OracleDriver";
        } else if ("sqlserver".equals(type)) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else {
            return "";
        }
    }
}
