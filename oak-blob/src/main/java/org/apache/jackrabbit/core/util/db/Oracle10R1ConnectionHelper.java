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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection helper for Oracle databases of version up to 10.1. It has special blob handling.
 */
public final class Oracle10R1ConnectionHelper extends OracleConnectionHelper {

    /**
     * the default logger
     */
    private static Logger log = LoggerFactory.getLogger(Oracle10R1ConnectionHelper.class);

    private Class<?> blobClass;

    private Integer durationSessionConstant;

    private Integer modeReadWriteConstant;

    /**
     * @param dataSrc the {@code DataSource} on which this helper acts
     * @param block whether to block on connection loss until the db is up again
     */
    public Oracle10R1ConnectionHelper(DataSource dataSrc, boolean block) {
        super(dataSrc, block);
    }

    /**
     * Retrieve the <code>oracle.sql.BLOB</code> class via reflection, and initialize the values for the
     * <code>DURATION_SESSION</code> and <code>MODE_READWRITE</code> constants defined there.
     */
    @Override
    public void init() throws Exception {
        super.init();
        // initialize oracle.sql.BLOB class & constants

        // use the Connection object for using the exact same
        // class loader that the Oracle driver was loaded with
        Connection con = null;
        try {
            con = dataSource.getConnection();
            blobClass = con.getClass().getClassLoader().loadClass("oracle.sql.BLOB");
            durationSessionConstant = new Integer(blobClass.getField("DURATION_SESSION").getInt(null));
            modeReadWriteConstant = new Integer(blobClass.getField("MODE_READWRITE").getInt(null));
        } finally {
            if (con != null) {
                DbUtility.close(con, null, null);
            }
        }
    }

    /**
     * Wraps any input-stream parameters in temporary blobs and frees these again after the statement
     * has been executed.
     * 
     * {@inheritDoc}
     */
    @Override
    protected PreparedStatement execute(PreparedStatement stmt, Object[] params) throws SQLException {
        List<Blob> tmpBlobs = new ArrayList<Blob>();
        try {
            for (int i = 0; params != null && i < params.length; i++) {
                Object p = params[i];
                if (p instanceof StreamWrapper) {
                    StreamWrapper wrapper = (StreamWrapper) p;
                    Blob tmp = createTemporaryBlob(stmt.getConnection(), wrapper.getStream());
                    tmpBlobs.add(tmp);
                    stmt.setBlob(i + 1, tmp);
                } else if (p instanceof InputStream) {
                    Blob tmp = createTemporaryBlob(stmt.getConnection(), (InputStream) p);
                    tmpBlobs.add(tmp);
                    stmt.setBlob(i + 1, tmp);
                } else {
                    stmt.setObject(i + 1, p);
                }
            }
            stmt.execute();
            return stmt;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
            for (Blob blob : tmpBlobs) {
                try {
                    freeTemporaryBlob(blob);
                } catch (Exception e) {
                    log.warn("Could not close temporary blob", e);
                }
            }
        }
    }

    /**
     * Creates a temporary oracle.sql.BLOB instance via reflection and spools the contents of the specified
     * stream.
     */
    private Blob createTemporaryBlob(Connection con, InputStream in) throws Exception {
        /*
         * BLOB blob = BLOB.createTemporary(con, false, BLOB.DURATION_SESSION);
         * blob.open(BLOB.MODE_READWRITE); OutputStream out = blob.getBinaryOutputStream(); ... out.flush();
         * out.close(); blob.close(); return blob;
         */
        Method createTemporary =
            blobClass.getMethod("createTemporary", new Class[]{Connection.class, Boolean.TYPE, Integer.TYPE});
        Object blob =
            createTemporary.invoke(null, new Object[]{ConnectionFactory.unwrap(con), Boolean.FALSE,
                    durationSessionConstant});
        Method open = blobClass.getMethod("open", new Class[]{Integer.TYPE});
        open.invoke(blob, new Object[]{modeReadWriteConstant});
        Method getBinaryOutputStream = blobClass.getMethod("getBinaryOutputStream", new Class[0]);
        OutputStream out = (OutputStream) getBinaryOutputStream.invoke(blob);
        try {
            IOUtils.copy(in, out);
        } finally {
            try {
                out.flush();
            } catch (IOException ioe) {
            }
            out.close();
        }
        Method close = blobClass.getMethod("close", new Class[0]);
        close.invoke(blob);
        return (Blob) blob;
    }

    /**
     * Frees a temporary oracle.sql.BLOB instance via reflection.
     */
    private void freeTemporaryBlob(Blob blob) throws Exception {
        // blob.freeTemporary();
        Method freeTemporary = blobClass.getMethod("freeTemporary", new Class[0]);
        freeTemporary.invoke(blob);
    }
}
