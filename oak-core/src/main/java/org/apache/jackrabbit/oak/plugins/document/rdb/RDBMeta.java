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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.CheckForNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDBMeta {

    private static final Logger LOG = LoggerFactory.getLogger(RDBMeta.class);

    /**
     * Tries to find a suitable binary type
     */
    @CheckForNull
    public static String findBinaryType(DatabaseMetaData md) throws SQLException {
        String binaryType = null;
        String dbtype = getDataBaseName(md);

        ResultSet rs = md.getTypeInfo();
        while (rs.next()) {
            String name = rs.getString(1);
            int type = rs.getInt(2);
            int size = rs.getInt(3);
            LOG.debug("Type information from " + dbtype + " -> " + name + " " + type + " " + size);
            if ((type == java.sql.Types.BINARY || type == java.sql.Types.VARBINARY) && (size == 0 || size > 65535)) {
                binaryType = name;
            }
        }
        return binaryType;
    }

    public static String getDataBaseName(DatabaseMetaData md) {
        try {
            return md.getDatabaseProductName() + " " + md.getDatabaseMajorVersion() + "." + md.getDatabaseMinorVersion() + " ("
                    + md.getDriverName() + " " + md.getDriverVersion() + ")";
        } catch (SQLException e) {
            return "(Could not determine DB type: " + e.getMessage() + ")";
        }
    }
}
