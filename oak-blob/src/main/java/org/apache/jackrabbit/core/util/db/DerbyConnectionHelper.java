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
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public final class DerbyConnectionHelper extends ConnectionHelper {

    /** name of the embedded driver */
    public static final String DERBY_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

    private static Logger log = LoggerFactory.getLogger(DerbyConnectionHelper.class);

    /**
     * @param dataSrc the {@code DataSource} on which this helper acts
     * @param block whether to block on connection loss until the db is up again
     */
    public DerbyConnectionHelper(DataSource dataSrc, boolean block) {
        super(dataSrc, block);
    }

    /**
     * Shuts the embedded Derby database down.
     * 
     * @param driver the driver
     * @throws SQLException on failure
     */
    public void shutDown(String driver) throws SQLException {
        // check for embedded driver
        if (!DERBY_EMBEDDED_DRIVER.equals(driver)) {
            return;
        }

        // prepare connection url for issuing shutdown command
        String url = null;
        Connection con = null;
        
        try {
            con = dataSource.getConnection();
            try {
                url = con.getMetaData().getURL();
            } catch (SQLException e) {
                // JCR-1557: embedded derby db probably already shut down;
                // this happens when configuring multiple FS/PM instances
                // to use the same embedded derby db instance.
                log.debug("failed to retrieve connection url: embedded db probably already shut down", e);
                return;
            }
            // we have to reset the connection to 'autoCommit=true' before closing it;
            // otherwise Derby would mysteriously complain about some pending uncommitted
            // changes which can't possibly be true.
            // @todo further investigate
            con.setAutoCommit(true);
        }
        finally {
            DbUtility.close(con, null, null);
        }
        int pos = url.lastIndexOf(';');
        if (pos != -1) {
            // strip any attributes from connection url
            url = url.substring(0, pos);
        }
        url += ";shutdown=true";

        // now it's safe to shutdown the embedded Derby database
        try {
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            // a shutdown command always raises a SQLException
            log.info(e.getMessage());
        }
    }
}
