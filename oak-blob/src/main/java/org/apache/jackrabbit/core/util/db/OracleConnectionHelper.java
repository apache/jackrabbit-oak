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
package org.apache.jackrabbit.core.util.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection helper for Oracle databases of version 10.2 and later.
 */
public class OracleConnectionHelper extends ConnectionHelper {

    /**
     * the default logger
     */
    private static Logger log = LoggerFactory.getLogger(OracleConnectionHelper.class);

    /**
     * @param dataSrc the {@code DataSource} on which this helper acts
     * @param block whether to block on connection loss until the db is up again
     */
    public OracleConnectionHelper(DataSource dataSrc, boolean block) {
        super(dataSrc, true, block);
    }

    /**
     * Initializes the helper: checks for valid driver version.
     * Subclasses that override this method should still call it!
     * 
     * @throws Exception on error
     */
    public void init() throws Exception {
         // check driver version
        Connection connection = dataSource.getConnection();
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            if (metaData.getDriverMajorVersion() < 10) {
                // Oracle drivers prior to version 10 only support
                // writing BLOBs up to 32k in size...
                log.warn("Unsupported driver version detected: "
                        + metaData.getDriverName()
                        + " v" + metaData.getDriverVersion());
            }
        } catch (SQLException e) {
            log.warn("Can not retrieve driver version", e);
        } finally {
            DbUtility.close(connection, null, null);
        }
    }

    /**
     * Since Oracle only supports table names up to 30 characters in
     * length illegal characters are simply replaced with "_" rather than
     * escaping them with "_x0000_".
     *
     * {@inheritDoc}
     */
    @Override
    protected final void replaceCharacter(StringBuilder escaped, char c) {
        escaped.append("_");
    }
}
