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

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.jackrabbit.core.data.AbstractDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.core.util.db.CheckSchemaOperation;
import org.apache.jackrabbit.core.util.db.ConnectionFactory;
import org.apache.jackrabbit.core.util.db.ConnectionHelper;
import org.apache.jackrabbit.core.util.db.DatabaseAware;
import org.apache.jackrabbit.core.util.db.DbUtility;
import org.apache.jackrabbit.core.util.db.StreamWrapper;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.WeakHashMap;

import javax.jcr.RepositoryException;
import javax.sql.DataSource;

/**
 * A data store implementation that stores the records in a database using JDBC.
 *
 * Configuration:
 * <pre>
 * &lt;DataStore class="org.apache.jackrabbit.core.data.db.DbDataStore"&gt;
 *     &lt;param name="{@link #setUrl(String) url}" value="jdbc:postgresql:test"/&gt;
 *     &lt;param name="{@link #setUser(String) user}" value="sa"/&gt;
 *     &lt;param name="{@link #setPassword(String) password}" value="sa"/&gt;
 *     &lt;param name="{@link #setDatabaseType(String) databaseType}" value="postgresql"/&gt;
 *     &lt;param name="{@link #setDriver(String) driver}" value="org.postgresql.Driver"/&gt;
 *     &lt;param name="{@link #setMinRecordLength(int) minRecordLength}" value="1024"/&gt;
 *     &lt;param name="{@link #setMaxConnections(int) maxConnections}" value="2"/&gt;
 *     &lt;param name="{@link #setCopyWhenReading(boolean) copyWhenReading}" value="true"/&gt;
 *     &lt;param name="{@link #setTablePrefix(String) tablePrefix}" value=""/&gt;
 *     &lt;param name="{@link #setSchemaObjectPrefix(String) schemaObjectPrefix}" value=""/&gt;
 *     &lt;param name="{@link #setSchemaCheckEnabled(boolean) schemaCheckEnabled}" value="true"/&gt;
 * &lt;/DataStore&gt;
 * </pre>
 * <p>
 * Only URL, user name and password usually need to be set.
 * The remaining settings are generated using the database URL sub-protocol from the
 * database type resource file.
 * <p>
 * JNDI can be used to get the connection. In this case, use the javax.naming.InitialContext as the driver,
 * and the JNDI name as the URL. If the user and password are configured in the JNDI resource,
 * they should not be configured here. Example JNDI settings:
 * <pre>
 * &lt;param name="driver" value="javax.naming.InitialContext" /&gt;
 * &lt;param name="url" value="java:comp/env/jdbc/Test" /&gt;
 * </pre>
 * <p>
 * For Microsoft SQL Server 2005, there is a problem reading large BLOBs. You will need to use
 * the JDBC driver version 1.2 or newer, and append ;responseBuffering=adaptive to the database URL.
 * Don't append ;selectMethod=cursor, otherwise it can still run out of memory.
 * Example database URL: jdbc:sqlserver://localhost:4220;DatabaseName=test;responseBuffering=adaptive
 * <p>
 * By default, the data is copied to a temp file when reading, to avoid problems when reading multiple
 * blobs at the same time.
 * <p>
 * The tablePrefix can be used to specify a schema and / or catalog name:
 * &lt;param name="tablePrefix" value="ds."&gt;
 */
public class DbDataStore extends AbstractDataStore
        implements DatabaseAware, MultiDataStoreAware {

    /**
     * The default value for the minimum object size.
     */
    public static final int DEFAULT_MIN_RECORD_LENGTH = 100;

    /**
     * Write to a temporary file to get the length (slow, but always works).
     * This is the default setting.
     */
    public static final String STORE_TEMP_FILE = "tempFile";

    /**
     * Call PreparedStatement.setBinaryStream(..., -1)
     */
    public static final String STORE_SIZE_MINUS_ONE = "-1";

    /**
     * Call PreparedStatement.setBinaryStream(..., Integer.MAX_VALUE)
     */
    public static final String STORE_SIZE_MAX = "max";

    /**
     * The digest algorithm used to uniquely identify records.
     */
    protected static final String DIGEST = System.getProperty("ds.digest.algorithm", "SHA-256");

    /**
     * The prefix used for temporary objects.
     */
    protected static final String TEMP_PREFIX = "TEMP_";

    /**
     * Logger instance
     */
    private static Logger log = LoggerFactory.getLogger(DbDataStore.class);

    /**
     * The minimum modified date. If a file is accessed (read or write) with a modified date
     * older than this value, the modified date is updated to the current time.
     */
    protected long minModifiedDate;

    /**
     * The database URL used.
     */
    protected String url;

    /**
     * The database driver.
     */
    protected String driver;

    /**
     * The user name.
     */
    protected String user;

    /**
     * The password
     */
    protected String password;

    /**
     * The database type used.
     */
    protected String databaseType;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    protected int minRecordLength = DEFAULT_MIN_RECORD_LENGTH;

    /**
     * The prefix for the datastore table, empty by default.
     */
    protected String tablePrefix = "";

    /**
     * The prefix of the table names. By default it is empty.
     */
    protected String schemaObjectPrefix = "";

    /**
     * Whether the schema check must be done during initialization.
     */
    private boolean schemaCheckEnabled = true;

    /**
     * The logical name of the DataSource to use.
     */
    protected String dataSourceName;

    /**
     * This is the property 'table'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String tableSQL = "DATASTORE";

    /**
     * This is the property 'createTable'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String createTableSQL =
        "CREATE TABLE ${tablePrefix}${table}(ID VARCHAR(255) PRIMARY KEY, LENGTH BIGINT, LAST_MODIFIED BIGINT, DATA BLOB)";

    /**
     * This is the property 'insertTemp'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String insertTempSQL =
        "INSERT INTO ${tablePrefix}${table} VALUES(?, 0, ?, NULL)";

    /**
     * This is the property 'updateData'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String updateDataSQL =
        "UPDATE ${tablePrefix}${table} SET DATA=? WHERE ID=?";

    /**
     * This is the property 'updateLastModified'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String updateLastModifiedSQL =
        "UPDATE ${tablePrefix}${table} SET LAST_MODIFIED=? WHERE ID=? AND LAST_MODIFIED<?";

    /**
     * This is the property 'update'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String updateSQL =
        "UPDATE ${tablePrefix}${table} SET ID=?, LENGTH=?, LAST_MODIFIED=? " +
        "WHERE ID=? AND LAST_MODIFIED=?";

    /**
     * This is the property 'delete'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String deleteSQL =
        "DELETE FROM ${tablePrefix}${table} WHERE ID=?";

    /**
     * This is the property 'deleteOlder'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String deleteOlderSQL =
        "DELETE FROM ${tablePrefix}${table} WHERE LAST_MODIFIED<?";

    /**
     * This is the property 'selectMeta'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String selectMetaSQL =
        "SELECT LENGTH, LAST_MODIFIED FROM ${tablePrefix}${table} WHERE ID=?";

    /**
     * This is the property 'selectAll'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String selectAllSQL =
        "SELECT ID FROM ${tablePrefix}${table}";

    /**
     * This is the property 'selectData'
     * in the [databaseType].properties file, initialized with the default value.
     */
    protected String selectDataSQL =
        "SELECT ID, DATA FROM ${tablePrefix}${table} WHERE ID=?";

    /**
     * The stream storing mechanism used.
     */
    protected String storeStream = STORE_TEMP_FILE;

    /**
     * Copy the stream to a temp file before returning it.
     * Enabled by default to support concurrent reads.
     */
    protected boolean copyWhenReading = true;

    /**
     * All data identifiers that are currently in use are in this set until they are garbage collected.
     */
    protected Map<DataIdentifier, WeakReference<DataIdentifier>> inUse =
        Collections.synchronizedMap(new WeakHashMap<DataIdentifier, WeakReference<DataIdentifier>>());

    /**
     * The temporary identifiers that are currently in use.
     */
    protected List<String> temporaryInUse = Collections.synchronizedList(new ArrayList<String>());

    /**
     * The {@link ConnectionHelper} set in the {@link #init(String)} method.
     * */
    protected ConnectionHelper conHelper;

    /**
     * The repositories {@link ConnectionFactory}.
     */
    private ConnectionFactory connectionFactory;

    public void setConnectionFactory(ConnectionFactory connnectionFactory) {
        this.connectionFactory = connnectionFactory;
    }

    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        InputStream fileInput = null;
        String tempId = null;
        ResultSet rs = null;
        try {
            long tempModified;
            while (true) {
                try {
                    tempModified = System.currentTimeMillis();
                    String id = UUID.randomUUID().toString();
                    tempId = TEMP_PREFIX + id;
                    temporaryInUse.add(tempId);
                    // SELECT LENGTH, LAST_MODIFIED FROM DATASTORE WHERE ID=?
                    rs = conHelper.query(selectMetaSQL, tempId);
                    boolean hasNext = rs.next();
                    DbUtility.close(rs);
                    rs = null;
                    if (hasNext) {
                        // re-try in the very, very unlikely event that the row already exists
                        continue;
                    }
                    // INSERT INTO DATASTORE VALUES(?, 0, ?, NULL)
                    conHelper.exec(insertTempSQL, tempId, tempModified);
                    break;
                } catch (Exception e) {
                    throw convert("Can not insert new record", e);
                } finally {
                    DbUtility.close(rs);
                    // prevent that rs.close() is called again
                    rs = null;
                }
            }
            MessageDigest digest = getDigest();
            DigestInputStream dIn = new DigestInputStream(stream, digest);
            CountingInputStream in = new CountingInputStream(dIn);
            StreamWrapper wrapper;
            if (STORE_SIZE_MINUS_ONE.equals(storeStream)) {
                wrapper = new StreamWrapper(in, -1);
            } else if (STORE_SIZE_MAX.equals(storeStream)) {
                wrapper = new StreamWrapper(in, Integer.MAX_VALUE);
            } else if (STORE_TEMP_FILE.equals(storeStream)) {
                File temp = moveToTempFile(in);
                long length = temp.length();
                wrapper = new StreamWrapper(new ResettableTempFileInputStream(temp), length);
            } else {
                throw new DataStoreException("Unsupported stream store algorithm: " + storeStream);
            }
            // UPDATE DATASTORE SET DATA=? WHERE ID=?
            conHelper.exec(updateDataSQL, wrapper, tempId);
            long length = in.getByteCount();
            DataIdentifier identifier =
                    new DataIdentifier(encodeHexString(digest.digest()));
            usesIdentifier(identifier);
            String id = identifier.toString();
            long newModified;
            while (true) {
                newModified = System.currentTimeMillis();
                if (checkExisting(tempId, length, identifier)) {
                    touch(identifier, newModified);
                    conHelper.exec(deleteSQL, tempId);
                    break;
                }
                try {
                    // UPDATE DATASTORE SET ID=?, LENGTH=?, LAST_MODIFIED=?
                    // WHERE ID=? AND LAST_MODIFIED=?
                    int count = conHelper.update(updateSQL,
                            id, length, newModified, tempId, tempModified);
                    // If update count is 0, the last modified time of the
                    // temporary row was changed - which means we need to
                    // re-try using a new last modified date (a later one)
                    // because we need to ensure the new last modified date
                    // is _newer_ than the old (otherwise the garbage
                    // collection could delete rows)
                    if (count != 0) {
                        // update was successful
                        break;
                    }
                } catch (SQLException e) {
                    // duplicate key (the row already exists) - repeat
                    // we use exception handling for flow control here, which is bad,
                    // but the alternative is to use UPDATE ... WHERE ... (SELECT ...)
                    // which could cause a deadlock in some databases - also,
                    // duplicate key will only occur if somebody else concurrently
                    // added the same record (which is very unlikely)
                }
                // SELECT LENGTH, LAST_MODIFIED FROM DATASTORE WHERE ID=?
                rs = conHelper.query(selectMetaSQL, tempId);
                if (!rs.next()) {
                    // the row was deleted, which is unexpected / not allowed
                    String msg =
                        DIGEST + " temporary entry deleted: " +
                            " id=" + tempId + " length=" + length;
                    log.error(msg);
                    throw new DataStoreException(msg);
                }
                tempModified = rs.getLong(2);
                DbUtility.close(rs);
                rs = null;
            }
            usesIdentifier(identifier);
            DbDataRecord record = new DbDataRecord(this, identifier, length, newModified);
            return record;
        } catch (Exception e) {
            throw convert("Can not insert new record", e);
        } finally {
            if (tempId != null) {
                temporaryInUse.remove(tempId);
            }
            DbUtility.close(rs);
            if (fileInput != null) {
                try {
                    fileInput.close();
                } catch (IOException e) {
                    throw convert("Can not close temporary file", e);
                }
            }
        }
    }

    /**
     * Check if a row with this ID already exists.
     *
     * @return true if the row exists and the length matches
     * @throw DataStoreException if a row exists, but the length is different
     */
    private boolean checkExisting(String tempId, long length, DataIdentifier identifier) throws DataStoreException, SQLException {
        String id = identifier.toString();
        // SELECT LENGTH, LAST_MODIFIED FROM DATASTORE WHERE ID=?
        ResultSet rs = null;
        try {
            rs = conHelper.query(selectMetaSQL, id);
            if (rs.next()) {
                long oldLength = rs.getLong(1);
                long lastModified = rs.getLong(2);
                if (oldLength != length) {
                    String msg =
                        DIGEST + " collision: temp=" + tempId
                        + " id=" + id + " length=" + length
                        + " oldLength=" + oldLength;
                    log.error(msg);
                    throw new DataStoreException(msg);
                }
                DbUtility.close(rs);
                rs = null;
                touch(identifier, lastModified);
                // row already exists
                conHelper.exec(deleteSQL, tempId);
                return true;
            }
        } finally {
            DbUtility.close(rs);
        }
        return false;
    }

    /**
     * Creates a temp file and copies the data there.
     * The input stream is closed afterwards.
     *
     * @param in the input stream
     * @return the file
     * @throws IOException
     */
    private File moveToTempFile(InputStream in) throws IOException {
        File temp = File.createTempFile("dbRecord", null);
        writeToFileAndClose(in, temp);
        return temp;
    }

    private void writeToFileAndClose(InputStream in, File file) throws IOException {
        OutputStream out = new FileOutputStream(file);
        try {
            IOUtils.copy(in, out);
        } finally {
            IOUtils.closeQuietly(out);
            IOUtils.closeQuietly(in);
        }
    }

    public synchronized void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        try {
            conHelper.exec(deleteSQL, identifier.toString());
        } catch (Exception e) {
            throw convert("Can not delete record", e);
        }
    }

    public synchronized int deleteAllOlderThan(long min) throws DataStoreException {
        try {
            ArrayList<String> touch = new ArrayList<String>();
            ArrayList<DataIdentifier> ids = new ArrayList<DataIdentifier>(inUse.keySet());
            for (DataIdentifier identifier: ids) {
                if (identifier != null) {
                    touch.add(identifier.toString());
                }
            }
            touch.addAll(temporaryInUse);
            for (String key : touch) {
                updateLastModifiedDate(key, 0);
            }
            // DELETE FROM DATASTORE WHERE LAST_MODIFIED<?
            log.debug(deleteOlderSQL + " - Scanstart [" + min + "]");
            return conHelper.update(deleteOlderSQL, min);
        } catch (Exception e) {
            throw convert("Can not delete records", e);
        }
    }

    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        ArrayList<DataIdentifier> list = new ArrayList<DataIdentifier>();
        ResultSet rs = null;
        try {
            // SELECT ID FROM DATASTORE
            rs = conHelper.query(selectAllSQL);
            while (rs.next()) {
                String id = rs.getString(1);
                if (!id.startsWith(TEMP_PREFIX)) {
                    DataIdentifier identifier = new DataIdentifier(id);
                    list.add(identifier);
                }
            }
            log.debug("Found " + list.size() + " identifiers.");
            return list.iterator();
        } catch (Exception e) {
            throw convert("Can not read records", e);
        } finally {
            DbUtility.close(rs);
        }
    }

    public int getMinRecordLength() {
        return minRecordLength;
    }

    /**
     * Set the minimum object length.
     * The maximum value is around 32000.
     *
     * @param minRecordLength the length
     */
    public void setMinRecordLength(int minRecordLength) {
        this.minRecordLength = minRecordLength;
    }

    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        usesIdentifier(identifier);
        ResultSet rs = null;
        try {
            String id = identifier.toString();
            // SELECT LENGTH, LAST_MODIFIED FROM DATASTORE WHERE ID = ?
            rs = conHelper.query(selectMetaSQL, id);
            if (!rs.next()) {
                return null;
            }
            long length = rs.getLong(1);
            long lastModified = rs.getLong(2);
            DbUtility.close(rs);
            rs = null;
            lastModified = touch(identifier, lastModified);
            return new DbDataRecord(this, identifier, length, lastModified);
        } catch (Exception e) {
            throw convert("Can not read identifier " + identifier, e);
        } finally {
            DbUtility.close(rs);
        }
    }

    /**
     * Open the input stream. This method sets those fields of the caller
     * that need to be closed once the input stream is read.
     *
     * @param inputStream the database input stream object
     * @param identifier data identifier
     * @throws DataStoreException if the data store could not be accessed,
     *          or if the given identifier is invalid
     */
    InputStream openStream(DbInputStream inputStream, DataIdentifier identifier) throws DataStoreException {
        ResultSet rs = null;
        try {
            // SELECT ID, DATA FROM DATASTORE WHERE ID = ?
            rs = conHelper.query(selectDataSQL, identifier.toString());
            if (!rs.next()) {
                throw new DataStoreException("Record not found: " + identifier);
            }
            InputStream stream = rs.getBinaryStream(2);
            if (stream == null) {
                stream = new ByteArrayInputStream(new byte[0]);
                DbUtility.close(rs);
            } else if (copyWhenReading) {
                // If we copy while reading, create a temp file and close the stream
                File temp = moveToTempFile(stream);
                stream = new BufferedInputStream(new TempFileInputStream(temp));
                DbUtility.close(rs);
            } else {
                stream = new BufferedInputStream(stream);
                inputStream.setResultSet(rs);
            }
            return stream;
        } catch (Exception e) {
            DbUtility.close(rs);
            throw convert("Retrieving database resource ", e);
        }
    }

    public synchronized void init(String homeDir) throws DataStoreException {
        try {
            initDatabaseType();

            conHelper = createConnectionHelper(getDataSource());

            if (isSchemaCheckEnabled()) {
                createCheckSchemaOperation().run();
            }
        } catch (Exception e) {
            throw convert("Can not init data store, driver=" + driver + " url=" + url + " user=" + user +
                    " schemaObjectPrefix=" + schemaObjectPrefix + " tableSQL=" + tableSQL + " createTableSQL=" + createTableSQL, e);
        }
    }

    private DataSource getDataSource() throws Exception {
        if (getDataSourceName() == null || "".equals(getDataSourceName())) {
            return connectionFactory.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
        } else {
            return connectionFactory.getDataSource(dataSourceName);
        }
    }

    /**
     * This method is called from the {@link #init(String)} method of this class and returns a
     * {@link ConnectionHelper} instance which is assigned to the {@code conHelper} field. Subclasses may
     * override it to return a specialized connection helper.
     *
     * @param dataSrc the {@link DataSource} of this persistence manager
     * @return a {@link ConnectionHelper}
     * @throws Exception on error
     */
    protected ConnectionHelper createConnectionHelper(DataSource dataSrc) throws Exception {
        return new ConnectionHelper(dataSrc, false);
    }

    /**
     * This method is called from {@link #init(String)} after the
     * {@link #createConnectionHelper(DataSource)} method, and returns a default {@link CheckSchemaOperation}.
     *
     * @return a new {@link CheckSchemaOperation} instance
     */
    protected final CheckSchemaOperation createCheckSchemaOperation() {
        String tableName = tablePrefix + schemaObjectPrefix + tableSQL;
        return new CheckSchemaOperation(conHelper, new ByteArrayInputStream(createTableSQL.getBytes()), tableName);
    }

    protected void initDatabaseType() throws DataStoreException {
        boolean failIfNotFound = false;
        if (databaseType == null) {
            if (dataSourceName != null) {
                try {
                    databaseType = connectionFactory.getDataBaseType(dataSourceName);
                } catch (RepositoryException e) {
                    throw new DataStoreException(e);
                }
            } else {
                if (!url.startsWith("jdbc:")) {
                    return;
                }
                int start = "jdbc:".length();
                int end = url.indexOf(':', start);
                databaseType = url.substring(start, end);
            }
        } else {
            failIfNotFound = true;
        }

        InputStream in =
            DbDataStore.class.getResourceAsStream(databaseType + ".properties");
        if (in == null) {
            if (failIfNotFound) {
                String msg =
                    "Configuration error: The resource '" + databaseType
                    + ".properties' could not be found;"
                    + " Please verify the databaseType property";
                log.debug(msg);
                throw new DataStoreException(msg);
            } else {
                return;
            }
        }
        Properties prop = new Properties();
        try {
            try {
                prop.load(in);
            } finally {
            in.close();
            }
        } catch (IOException e) {
            String msg = "Configuration error: Could not read properties '" + databaseType + ".properties'";
            log.debug(msg);
            throw new DataStoreException(msg, e);
        }
        if (driver == null) {
            driver = getProperty(prop, "driver", driver);
        }
        tableSQL = getProperty(prop, "table", tableSQL);
        createTableSQL = getProperty(prop, "createTable", createTableSQL);
        insertTempSQL = getProperty(prop, "insertTemp", insertTempSQL);
        updateDataSQL = getProperty(prop, "updateData", updateDataSQL);
        updateLastModifiedSQL = getProperty(prop, "updateLastModified", updateLastModifiedSQL);
        updateSQL = getProperty(prop, "update", updateSQL);
        deleteSQL = getProperty(prop, "delete", deleteSQL);
        deleteOlderSQL = getProperty(prop, "deleteOlder", deleteOlderSQL);
        selectMetaSQL = getProperty(prop, "selectMeta", selectMetaSQL);
        selectAllSQL = getProperty(prop, "selectAll", selectAllSQL);
        selectDataSQL = getProperty(prop, "selectData", selectDataSQL);
        storeStream = getProperty(prop, "storeStream", storeStream);
        if (!STORE_SIZE_MINUS_ONE.equals(storeStream)
                && !STORE_TEMP_FILE.equals(storeStream)
                && !STORE_SIZE_MAX.equals(storeStream)) {
            String msg = "Unsupported Stream store mechanism: " + storeStream
                    + " supported are: " + STORE_SIZE_MINUS_ONE + ", "
                    + STORE_TEMP_FILE + ", " + STORE_SIZE_MAX;
            log.debug(msg);
            throw new DataStoreException(msg);
        }
    }

    /**
     * Get the expanded property value. The following placeholders are supported:
     * ${table}: the table name (the default is DATASTORE) and
     * ${tablePrefix}: tablePrefix plus schemaObjectPrefix as set in the configuration
     *
     * @param prop the properties object
     * @param key the key
     * @param defaultValue the default value
     * @return the property value (placeholders are replaced)
     */
    protected String getProperty(Properties prop, String key, String defaultValue) {
        String sql = prop.getProperty(key, defaultValue);
        sql = Text.replace(sql, "${table}", tableSQL).trim();
        sql = Text.replace(sql, "${tablePrefix}", tablePrefix + schemaObjectPrefix).trim();
        return sql;
    }

    /**
     * Convert an exception to a data store exception.
     *
     * @param cause the message
     * @param e the root cause
     * @return the data store exception
     */
    protected DataStoreException convert(String cause, Exception e) {
        log.warn(cause, e);
        if (e instanceof DataStoreException) {
            return (DataStoreException) e;
        } else {
            return new DataStoreException(cause, e);
        }
    }

    public void updateModifiedDateOnAccess(long before) {
        log.debug("Update modifiedDate on access before " + before);
        minModifiedDate = before;
    }

    /**
     * Update the modified date of an entry if required.
     *
     * @param identifier the entry identifier
     * @param lastModified the current last modified date
     * @return the new modified date
     */
    long touch(DataIdentifier identifier, long lastModified) throws DataStoreException {
        usesIdentifier(identifier);
        return updateLastModifiedDate(identifier.toString(), lastModified);
    }

    private long updateLastModifiedDate(String key, long lastModified) throws DataStoreException {
        if (lastModified < minModifiedDate) {
            long now = System.currentTimeMillis();
            try {
                // UPDATE DATASTORE SET LAST_MODIFIED = ? WHERE ID = ? AND LAST_MODIFIED < ?
                conHelper.update(updateLastModifiedSQL, now, key, now);
                return now;
            } catch (Exception e) {
                throw convert("Can not update lastModified", e);
            }
        }
        return lastModified;
    }

    /**
     * Get the database type (if set).
     * @return the database type
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * Set the database type. By default the sub-protocol of the JDBC database URL is used if it is not set.
     * It must match the resource file [databaseType].properties. Example: mysql.
     *
     * @param databaseType
     */
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    /**
     * Get the database driver
     *
     * @return the driver
     */
    public String getDriver() {
        return driver;
    }

    /**
     * Set the database driver class name.
     * If not set, the default driver class name for the database type is used,
     * as set in the [databaseType].properties resource; key 'driver'.
     *
     * @param driver
     */
    public void setDriver(String driver) {
        this.driver = driver;
    }

    /**
     * Get the password.
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Set the password.
     *
     * @param password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Get the database URL.
     *
     * @return the URL
     */
    public String getUrl() {
        return url;
    }

    /**
     * Set the database URL.
     * Example: jdbc:postgresql:test
     *
     * @param url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Get the user name.
     *
     * @return the user name
     */
    public String getUser() {
        return user;
    }

    /**
     * Set the user name.
     *
     * @param user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return whether the schema check is enabled
     */
    public final boolean isSchemaCheckEnabled() {
        return schemaCheckEnabled;
    }

    /**
     * @param enabled set whether the schema check is enabled
     */
    public final void setSchemaCheckEnabled(boolean enabled) {
        schemaCheckEnabled = enabled;
    }

    public synchronized void close() throws DataStoreException {
        // nothing to do
    }

    protected void usesIdentifier(DataIdentifier identifier) {
        inUse.put(identifier, new WeakReference<DataIdentifier>(identifier));
    }

    public void clearInUse() {
        inUse.clear();
    }

    protected synchronized MessageDigest getDigest() throws DataStoreException {
        try {
            return MessageDigest.getInstance(DIGEST);
        } catch (NoSuchAlgorithmException e) {
            throw convert("No such algorithm: " + DIGEST, e);
        }
    }

    /**
     * Get the maximum number of concurrent connections.
     *
     * @deprecated
     * @return the maximum number of connections.
     */
    @Deprecated
    public int getMaxConnections() {
        return -1;
    }

    /**
     * Set the maximum number of concurrent connections in the pool.
     * At least 3 connections are required if the garbage collection process is used.
     *
     * @deprecated
     * @param maxConnections the new value
     */
    @Deprecated
    public void setMaxConnections(int maxConnections) {
        // no effect
    }

    /**
     * Is a stream copied to a temporary file before returning?
     *
     * @return the setting
     */
    public boolean getCopyWhenReading() {
        return copyWhenReading;
    }

    /**
     * The the copy setting. If enabled,
     * a stream is always copied to a temporary file when reading a stream.
     *
     * @param copyWhenReading the new setting
     */
    public void setCopyWhenReading(boolean copyWhenReading) {
        this.copyWhenReading = copyWhenReading;
    }

    /**
     * Get the table prefix.
     *
     * @return the table prefix.
     */
    public String getTablePrefix() {
        return tablePrefix;
    }

    /**
     * Set the new table prefix. The default is empty.
     * The table name is constructed like this:
     * ${tablePrefix}${schemaObjectPrefix}${tableName}
     *
     * @param tablePrefix the new value
     */
    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    /**
     * Get the schema prefix.
     *
     * @return the schema object prefix
     */
    public String getSchemaObjectPrefix() {
        return schemaObjectPrefix;
    }

    /**
     * Set the schema object prefix. The default is empty.
     * The table name is constructed like this:
     * ${tablePrefix}${schemaObjectPrefix}${tableName}
     *
     * @param schemaObjectPrefix the new prefix
     */
    public void setSchemaObjectPrefix(String schemaObjectPrefix) {
        this.schemaObjectPrefix = schemaObjectPrefix;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }
}
