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
package org.apache.jackrabbit.core.data.db;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;

import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.util.db.DbUtility;

/**
 * This class represents an input stream backed by a database. The database
 * objects are only acquired when reading from the stream, and stay open until
 * the stream is closed, fully read, or garbage collected.
 * <p>
 * This class does not support mark/reset. It is always to be wrapped
 * using a BufferedInputStream.
 */
public class DbInputStream extends AutoCloseInputStream {

    protected DbDataStore store;
    protected DataIdentifier identifier;
    protected boolean endOfStream;

    protected ResultSet rs;
    private InputStream initialStream;

    /**
     * Create a database input stream for the given identifier.
     * Database access is delayed until the first byte is read from the stream.
     *
     * @param store the database data store
     * @param identifier the data identifier
     */
    protected DbInputStream(DbDataStore store, DataIdentifier identifier) {
        super(null);
        this.store = store;
        this.identifier = identifier;
        this.initialStream = super.in;
    }

    /**
     * Open the stream if required.
     *
     * @throws IOException
     */
    protected void openStream() throws IOException {
        if (endOfStream) {
            throw new EOFException();
        }
        if (in == initialStream) {
            try {
                in = store.openStream(this, identifier);
            } catch (DataStoreException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * When the stream is consumed, the database objects held by the instance are closed.
     */
    public int read() throws IOException {
        if (endOfStream) {
            return -1;
        }
        openStream();
        int c = in.read();
        if (c == -1) {
            endOfStream = true;
            close();
        }
        return c;
    }

    /**
     * {@inheritDoc}
     * When the stream is consumed, the database objects held by the instance are closed.
     */
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * {@inheritDoc}
     * When the stream is consumed, the database objects held by the instance are closed.
     */
    public int read(byte[] b, int off, int len) throws IOException {
        if (endOfStream) {
            return -1;
        }
        openStream();
        int c = in.read(b, off, len);
        if (c == -1) {
            endOfStream = true;
            close();
        }
        return c;
    }

    /**
     * {@inheritDoc}
     * When the stream is consumed, the database objects held by the instance are closed.
     */
    public void close() throws IOException {
        if (in != initialStream) {
            in.close();
            in = initialStream;
            // some additional database objects
            // may need to be closed
            if (rs != null) {
                DbUtility.close(rs);
                rs = null;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public long skip(long n) throws IOException {
        if (endOfStream) {
            return -1;
        }
        openStream();
        return in.skip(n);
    }

    /**
     * {@inheritDoc}
     */
    public int available() throws IOException {
        if (endOfStream) {
            return 0;
        }
        openStream();
        return in.available();
    }

    /**
     * This method does nothing.
     */
    public void mark(int readlimit) {
        // do nothing
    }

    /**
     * This method does nothing.
     */
    public void reset() throws IOException {
        // do nothing
    }

    /**
     * Check whether mark and reset are supported.
     *
     * @return false
     */
    public boolean markSupported() {
        return false;
    }

    /**
     * Set the result set of this input stream. This object must be closed once
     * the stream is closed.
     *
     * @param rs the result set
     */
    void setResultSet(ResultSet rs) {
        this.rs = rs;
    }
}
