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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.document.UpdateUtils.checkConditions;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeResultSet;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeStatement;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.createTableName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStoreDB.FETCHFIRSTSYNTAX;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.util.OakVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.util.concurrent.Striped;

/**
 * Implementation of {@link DocumentStore} for relational databases.
 * 
 * <h3>Supported Databases</h3>
 * <p>
 * The code is supposed to be sufficiently generic to run with a variety of
 * database implementations. However, the tables are created when required to
 * simplify testing, and <em>that</em> code specifically supports these
 * databases:
 * <ul>
 * <li>h2</li>
 * <li>IBM DB2</li>
 * <li>Postgres</li>
 * <li>MariaDB (MySQL) (experimental)</li>
 * <li>Oracle (experimental)</li>
 * </ul>
 * 
 * <h3>Table Layout</h3>
 * <p>
 * Data for each of the DocumentStore's {@link Collection}s is stored in its own
 * database table (with a name matching the collection).
 * <p>
 * The tables essentially implement key/value storage, where the key usually is
 * derived from an Oak path, and the value is a serialization of a
 * {@link Document} (or a part of one). Additional fields are used for queries,
 * debugging, and concurrency control:
 * <table style="text-align: left;">
 * <thead>
 * <tr>
 * <th>Column</th>
 * <th>Type</th>
 * <th>Description</th>
 * </tr>
 * </thead> <tbody>
 * <tr>
 * <th>ID</th>
 * <td>varchar(512) not null primary key</td>
 * <td>the document's key (for databases that can not handle 512 character
 * primary keys, such as MySQL, varbinary is possible as well; note that this
 * currently needs to be hardcoded)</td>
 * </tr>
 * <tr>
 * <th>MODIFIED</th>
 * <td>bigint</td>
 * <td>low-resolution timestamp
 * </tr>
 * <tr>
 * <th>HASBINARY</th>
 * <td>smallint</td>
 * <td>flag indicating whether the document has binary properties
 * </tr>
 * <tr>
 * <th>DELETEDONCE</th>
 * <td>smallint</td>
 * <td>flag indicating whether the document has been deleted once
 * </tr>
 * <tr>
 * <th>MODCOUNT</th>
 * <td>bigint</td>
 * <td>modification counter, used for avoiding overlapping updates</td>
 * </tr>
 * <tr>
 * <th>DSIZE</th>
 * <td>bigint</td>
 * <td>the approximate size of the document's JSON serialization (for debugging
 * purposes)</td>
 * </tr>
 * <tr>
 * <th>DATA</th>
 * <td>varchar(16384)</td>
 * <td>the document's JSON serialization (only used for small document sizes, in
 * which case BDATA (below) is not set), or a sequence of JSON serialized update
 * operations to be applied against the last full serialization</td>
 * </tr>
 * <tr>
 * <th>BDATA</th>
 * <td>blob</td>
 * <td>the document's JSON serialization (usually GZIPped, only used for "large"
 * documents)</td>
 * </tr>
 * </tbody>
 * </table>
 * <p>
 * The names of database tables can be prefixed; the purpose is mainly for
 * testing, as tables can also be dropped automatically when the store is
 * disposed (this only happens for those tables that have been created on
 * demand)
 * <p>
 * <em>Note that the database needs to be created/configured to support all Unicode
 * characters in text fields, and to collate by Unicode code point (in DB2: "collate using identity",
 * in Postgres: "C").
 * THIS IS NOT THE DEFAULT!</em>
 * <p>
 * <em>For MySQL, the database parameter "max_allowed_packet" needs to be increased to support ~16 blobs.</em>
 * 
 * <h3>Caching</h3>
 * <p>
 * The cache borrows heavily from the {@link MongoDocumentStore} implementation;
 * however it does not support the off-heap mechanism yet.
 * 
 * <h3>Queries</h3>
 * <p>
 * The implementation currently supports only three indexed properties:
 * "_bin", "deletedOnce", and "_modified". Attempts to use a different indexed property will
 * cause a {@link DocumentStoreException}.
 */
public class RDBDocumentStore implements DocumentStore {

    /**
     * Creates a {@linkplain RDBDocumentStore} instance using the provided
     * {@link DataSource}, {@link DocumentMK.Builder}, and {@link RDBOptions}.
     */
    public RDBDocumentStore(DataSource ds, DocumentMK.Builder builder, RDBOptions options) {
        try {
            initialize(ds, builder, options);
        } catch (Exception ex) {
            throw new DocumentStoreException("initializing RDB document store", ex);
        }
    }

    /**
     * Creates a {@linkplain RDBDocumentStore} instance using the provided
     * {@link DataSource}, {@link DocumentMK.Builder}, and default
     * {@link RDBOptions}.
     */
    public RDBDocumentStore(DataSource ds, DocumentMK.Builder builder) {
        this(ds, builder, new RDBOptions());
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String id) {
        return find(collection, id, Integer.MAX_VALUE);
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection, final String id, int maxCacheAge) {
        return readDocumentCached(collection, id, maxCacheAge);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty,
            long startValue, int limit) {
        return internalQuery(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String id) {
        delete(collection, id);
        invalidateCache(collection, id, true);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> ids) {
        for (String id : ids) {
            invalidateCache(collection, id, true);
        }
        delete(collection, ids);
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                            Map<String, Map<Key, Condition>> toRemove) {
        int num = delete(collection, toRemove);
        for (String id : toRemove.keySet()) {
            invalidateCache(collection, id, true);
        }
        return num;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        return internalCreate(collection, updateOps);
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        internalUpdate(collection, keys, updateOp);
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        return internalCreateOrUpdate(collection, update, true, false);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        return internalCreateOrUpdate(collection, update, false, true);
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        for (NodeDocument nd : nodesCache.asMap().values()) {
            nd.markUpToDate(0);
        }
        return null;
    }
    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        //TODO: optimize me
        return invalidateCache();
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String id) {
        invalidateCache(collection, id, false);
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String tableName = getTable(Collection.NODES).getName();
        long result;
        try {
            connection = this.ch.getROConnection();
            String t = "select ";
            if (this.db.getFetchFirstSyntax() == FETCHFIRSTSYNTAX.TOP) {
                t += "TOP 1 ";
            }
            t += this.db.getCurrentTimeStampInMsSyntax() + " from " + tableName;
            switch (this.db.getFetchFirstSyntax()) {
                case LIMIT:
                    t += " LIMIT 1";
                    break;
                case FETCHFIRST:
                    t += " FETCH FIRST 1 ROWS ONLY";
                    break;
                default:
                    break;
            }

            stmt = connection.prepareStatement(t);
            long start = System.currentTimeMillis();
            rs = stmt.executeQuery();
            if (rs.next()) {
                long roundtrip = System.currentTimeMillis() - start;
                long serverTime = rs.getTimestamp(1).getTime();
                result = (start + roundtrip / 2) - serverTime;
            } else {
                throw new DocumentStoreException("failed to determine server timestamp");
            }
            connection.commit();
            return result;
        } catch (Exception ex) {
            LOG.error("", ex);
            throw new DocumentStoreException(ex);
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
            this.ch.closeConnection(connection);
        }
    }

    private <T extends Document> void invalidateCache(Collection<T> collection, String id, boolean remove) {
        if (collection == Collection.NODES) {
            invalidateNodesCache(id, remove);
        }
    }

    private void invalidateNodesCache(String id, boolean remove) {
        StringValue key = new StringValue(id);
        Lock lock = getAndLock(id);
        try {
            if (remove) {
                nodesCache.invalidate(key);
            } else {
                NodeDocument entry = nodesCache.getIfPresent(key);
                if (entry != null) {
                    entry.markUpToDate(0);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // used for diagnostics
    private String droppedTables = "";

    public String getDroppedTables() {
        return this.droppedTables;
    }

    // table names
    private static Map<Collection<? extends Document>, String> TABLEMAP;
    private static List<String> TABLENAMES;
    static {
        Map<Collection<? extends Document>, String> tmp = new HashMap<Collection<? extends Document>, String>();
        tmp.put(Collection.CLUSTER_NODES, "CLUSTERNODES");
        tmp.put(Collection.JOURNAL, "JOURNAL");
        tmp.put(Collection.NODES, "NODES");
        tmp.put(Collection.SETTINGS, "SETTINGS");
        TABLEMAP = Collections.unmodifiableMap(tmp);
        List<String> tl = new ArrayList<String>(TABLEMAP.values());
        Collections.sort(tl);
        TABLENAMES = Collections.unmodifiableList(tl);
    }

    public static List<String> getTableNames() {
        return TABLENAMES;
    }

    /**
     * Holds the data about a table that can vary: name, whether the primary key
     * is binary, and the estimated size of the "data" column.
     */
    private static class TableMetaData {

        final String name;
        boolean idIsBinary = false;
        private int dataLimitInOctets = 16384;

        public TableMetaData(String name) {
            this.name = name;
        }

        public int getDataLimitInOctets() {
            return this.dataLimitInOctets;
        }

        public String getName() {
            return this.name;
        }

        public boolean isIdBinary() {
            return this.idIsBinary;
        }

        public void setIdIsBinary(boolean idIsBinary) {
            this.idIsBinary = idIsBinary;
        }

        public void setDataLimitInOctets(int dataLimitInOctets) {
            this.dataLimitInOctets = dataLimitInOctets;
        }
    }

    private final Map<Collection<? extends Document>, TableMetaData> tableMeta = new HashMap<Collection<? extends Document>, TableMetaData>();

    @Override
    public void dispose() {
        if (!this.tablesToBeDropped.isEmpty()) {
            String dropped = "";
            LOG.debug("attempting to drop: " + this.tablesToBeDropped);
            for (String tname : this.tablesToBeDropped) {
                Connection con = null;
                try {
                    con = this.ch.getRWConnection();
                    Statement stmt = null;
                    try {
                        stmt = con.createStatement();
                        stmt.execute("drop table " + tname);
                        stmt.close();
                        con.commit();
                        dropped += tname + " ";
                    } catch (SQLException ex) {
                        LOG.debug("attempting to drop: " + tname, ex);
                    } finally {
                        closeStatement(stmt);
                    }
                } catch (SQLException ex) {
                    LOG.debug("attempting to drop: " + tname, ex);
                } finally {
                    this.ch.closeConnection(con);
                }
            }
            this.droppedTables = dropped.trim();
        }
        try {
            this.ch.close();
        } catch (IOException ex) {
            LOG.error("closing connection handler", ex);
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String id) {
        if (collection != Collection.NODES) {
            return null;
        } else {
            NodeDocument doc = nodesCache.getIfPresent(new StringValue(id));
            return castAsT(doc);
        }
    }

    @Override
    public CacheStats getCacheStats() {
        return this.cacheStats;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    // implementation

    private static final String MODIFIED = "_modified";
    private static final String MODCOUNT = "_modCount";

    /**
     * Optional counter for changes to "_collisions" map ({@link NodeDocument#COLLISIONS}).
     */
    private static final String COLLISIONSMODCOUNT = "_collisionsModCount";

    private static final String ID = "_id";

    private static final Logger LOG = LoggerFactory.getLogger(RDBDocumentStore.class);

    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    private Exception callStack;

    private RDBConnectionHandler ch;

    // from options
    private Set<String> tablesToBeDropped = new HashSet<String>();

    // ratio between Java characters and UTF-8 encoding
    // a) single characters will fit into 3 bytes
    // b) a surrogate pair (two Java characters) will fit into 4 bytes
    // thus...
    private static final int CHAR2OCTETRATIO = 3;

    // number of retries for updates
    private static final int RETRIES = 10;

    // see OAK-2044
    protected static final boolean USECMODCOUNT = true;

    private static final Key MODIFIEDKEY = new Key(MODIFIED, null);

    // DB-specific information
    private RDBDocumentStoreDB db;

    private Map<String, String> metadata;

    // set of supported indexed properties
    private static final Set<String> INDEXEDPROPERTIES = new HashSet<String>(Arrays.asList(new String[] { MODIFIED,
            NodeDocument.HAS_BINARY_FLAG, NodeDocument.DELETED_ONCE }));

    // set of properties not serialized to JSON
    private static final Set<String> COLUMNPROPERTIES = new HashSet<String>(Arrays.asList(new String[] { ID,
            NodeDocument.HAS_BINARY_FLAG, NodeDocument.DELETED_ONCE, COLLISIONSMODCOUNT, MODIFIED, MODCOUNT }));

    private final RDBDocumentSerializer SR = new RDBDocumentSerializer(this, COLUMNPROPERTIES);

    private void initialize(DataSource ds, DocumentMK.Builder builder, RDBOptions options) throws Exception {

        this.tableMeta.put(Collection.NODES,
                new TableMetaData(createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.NODES))));
        this.tableMeta.put(Collection.CLUSTER_NODES,
                new TableMetaData(createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.CLUSTER_NODES))));
        this.tableMeta.put(Collection.JOURNAL,
                new TableMetaData(createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.JOURNAL))));
        this.tableMeta.put(Collection.SETTINGS,
                new TableMetaData(createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.SETTINGS))));

        this.ch = new RDBConnectionHandler(ds);
        this.callStack = LOG.isDebugEnabled() ? new Exception("call stack of RDBDocumentStore creation") : null;

        this.nodesCache = builder.buildDocumentCache(this);
        this.cacheStats = new CacheStats(nodesCache, "Document-Documents", builder.getWeigher(), builder.getDocumentCacheSize());

        Connection con = this.ch.getRWConnection();

        int isolation = con.getTransactionIsolation();
        String isolationDiags = RDBJDBCTools.isolationLevelToString(isolation);
        if (isolation != Connection.TRANSACTION_READ_COMMITTED) {
            LOG.info("Detected transaction isolation level " + isolationDiags + " is "
                    + (isolation < Connection.TRANSACTION_READ_COMMITTED ? "lower" : "higher") + " than expected "
                    + RDBJDBCTools.isolationLevelToString(Connection.TRANSACTION_READ_COMMITTED)
                    + " - check datasource configuration");
        }

        DatabaseMetaData md = con.getMetaData();
        String dbDesc = String.format("%s %s (%d.%d)", md.getDatabaseProductName(), md.getDatabaseProductVersion(),
                md.getDatabaseMajorVersion(), md.getDatabaseMinorVersion()).replaceAll("[\r\n\t]", " ").trim();
        String driverDesc = String.format("%s %s (%d.%d)", md.getDriverName(), md.getDriverVersion(), md.getDriverMajorVersion(),
                md.getDriverMinorVersion()).replaceAll("[\r\n\t]", " ").trim();
        String dbUrl = md.getURL();

        this.db = RDBDocumentStoreDB.getValue(md.getDatabaseProductName());
        this.metadata = ImmutableMap.<String,String>builder()
                .put("type", "rdb")
                .put("db", md.getDatabaseProductName())
                .put("version", md.getDatabaseProductVersion())
                .build();
        String versionDiags = db.checkVersion(md);
        if (!versionDiags.isEmpty()) {
            LOG.info(versionDiags);
        }

        if (! "".equals(db.getInitializationStatement())) {
            Statement stmt = null;
            try {
                stmt = con.createStatement();
                stmt.execute(db.getInitializationStatement());
                stmt.close();
                con.commit();
            }
            finally {
                closeStatement(stmt);
            }
        }

        List<String> tablesCreated = new ArrayList<String>();
        List<String> tablesPresent = new ArrayList<String>();
        StringBuilder tableDiags = new StringBuilder();
        try {
            createTableFor(con, Collection.CLUSTER_NODES, this.tableMeta.get(Collection.CLUSTER_NODES), tablesCreated,
                    tablesPresent, tableDiags);
            createTableFor(con, Collection.NODES, this.tableMeta.get(Collection.NODES), tablesCreated, tablesPresent,
                    tableDiags);
            createTableFor(con, Collection.SETTINGS, this.tableMeta.get(Collection.SETTINGS), tablesCreated, tablesPresent,
                    tableDiags);
            createTableFor(con, Collection.JOURNAL, this.tableMeta.get(Collection.JOURNAL), tablesCreated, tablesPresent,
                    tableDiags);
        } finally {
            con.commit();
            con.close();
        }

        if (options.isDropTablesOnClose()) {
            tablesToBeDropped.addAll(tablesCreated);
        }

        if (tableDiags.length() != 0) {
            tableDiags.insert(0, ", ");
        }

        String diag = db.getAdditionalDiagnostics(this.ch, this.tableMeta.get(Collection.NODES).getName());

        LOG.info("RDBDocumentStore (" + OakVersion.getVersion() + ") instantiated for database " + dbDesc + ", using driver: "
                + driverDesc + ", connecting to: " + dbUrl + (diag.isEmpty() ? "" : (", properties: " + diag))
                + ", transaction isolation level: " + isolationDiags + tableDiags);
        if (!tablesPresent.isEmpty()) {
            LOG.info("Tables present upon startup: " + tablesPresent);
        }
        if (!tablesCreated.isEmpty()) {
            LOG.info("Tables created upon startup: " + tablesCreated
                    + (options.isDropTablesOnClose() ? " (will be dropped on exit)" : ""));
        }
    }

    private static boolean isBinaryType(int sqlType) {
        return sqlType == Types.VARBINARY || sqlType == Types.BINARY || sqlType == Types.LONGVARBINARY;
    }

    private void obtainFlagsFromResultSetMeta(ResultSetMetaData met, TableMetaData tmd) throws SQLException {
        for (int i = 1; i <= met.getColumnCount(); i++) {
            String lcName = met.getColumnName(i).toLowerCase(Locale.ENGLISH);
            if ("id".equals(lcName)) {
                tmd.setIdIsBinary(isBinaryType(met.getColumnType(i)));
            }
            if ("data".equals(lcName)) {
                tmd.setDataLimitInOctets(met.getPrecision(i));
            }
        }
    }

    private static String asQualifiedDbName(String one, String two) {
        if (one == null && two == null) {
            return null;
        }
        else {
            one = one == null ? "" : one.trim();
            two = two == null ? "" : two.trim();
            return one.isEmpty() ? two : one + "." + two;
        }
    }

    private static String indexTypeAsString(int type) {
        switch (type) {
            case DatabaseMetaData.tableIndexClustered:
                return "clustered";
            case DatabaseMetaData.tableIndexHashed:
                return "hashed";
            case DatabaseMetaData.tableIndexStatistic:
                return "statistic";
            case DatabaseMetaData.tableIndexOther:
                return "other";
            default:
                return "indexType=" + type;
        }
    }

    private String dumpIndexData(DatabaseMetaData met, ResultSetMetaData rmet, String tableName) {

        ResultSet rs = null;
        try {
            // if the result set metadata provides a table name, use that (the other one
            // might be inaccurate due to case insensitivity issues
            String rmetTableName = rmet.getTableName(1);
            if (rmetTableName != null && !rmetTableName.trim().isEmpty()) {
                tableName = rmetTableName.trim();
            }

            String rmetSchemaName = rmet.getSchemaName(1);
            rmetSchemaName = rmetSchemaName == null ? "" : rmetSchemaName.trim();

            Map<String, Map<String, Object>> indices = new TreeMap<String, Map<String, Object>>();
            StringBuilder sb = new StringBuilder();
            rs = met.getIndexInfo(null, null, tableName, false, true);
            while (rs.next()) {
                String name = asQualifiedDbName(rs.getString(5), rs.getString(6));
                if (name != null) {
                    Map<String, Object> info = indices.get(name);
                    if (info == null) {
                        info = new HashMap<String, Object>();
                        indices.put(name, info);
                        info.put("fields", new TreeMap<Integer, String>());
                    }
                    info.put("nonunique", rs.getBoolean(4));
                    info.put("type", indexTypeAsString(rs.getInt(7)));
                    String inSchema = rs.getString(2);
                    inSchema = inSchema == null ? "" : inSchema.trim();
                    // skip indices on tables in other schemas in case we have that information
                    if (rmetSchemaName.isEmpty() || inSchema.isEmpty() || rmetSchemaName.equals(inSchema)) {
                        String tname = asQualifiedDbName(inSchema, rs.getString(3));
                        info.put("tname", tname);
                        String cname = rs.getString(9);
                        if (cname != null) {
                            String order = "A".equals(rs.getString(10)) ? " ASC" : ("D".equals(rs.getString(10)) ? " DESC" : "");
                            ((Map<Integer, String>) info.get("fields")).put(rs.getInt(8), cname + order);
                        }
                    }
                }
            }
            for (Entry<String, Map<String, Object>> index : indices.entrySet()) {
                boolean nonUnique = ((Boolean) index.getValue().get("nonunique"));
                Map<Integer, String> fields = (Map<Integer, String>) index.getValue().get("fields");
                if (!fields.isEmpty()) {
                    if (sb.length() != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("%sindex %s on %s (", nonUnique ? "" : "unique ", index.getKey(),
                            index.getValue().get("tname")));
                    String delim = "";
                    for (String field : fields.values()) {
                        sb.append(delim);
                        delim = ", ";
                        sb.append(field);
                    }
                    sb.append(")");
                    sb.append(" ").append(index.getValue().get("type"));
                }
            }
            if (sb.length() != 0) {
                sb.insert(0, "/* ").append(" */");
            }
            return sb.toString();
        } catch (SQLException ex) {
            // well it was best-effort
            return "";
        } finally {
            closeResultSet(rs);
        }
    }

    private void createTableFor(Connection con, Collection<? extends Document> col, TableMetaData tmd, List<String> tablesCreated,
            List<String> tablesPresent, StringBuilder diagnostics) throws SQLException {
        String dbname = this.db.toString();
        if (con.getMetaData().getURL() != null) {
            dbname += " (" + con.getMetaData().getURL() + ")";
        }
        String tableName = tmd.getName();

        PreparedStatement checkStatement = null, checkStatement2 = null;

        ResultSet checkResultSet = null;
        Statement creatStatement = null;
        try {
            checkStatement = con.prepareStatement("select * from " + tableName + " where ID = ?");
            checkStatement.setString(1, "0:/");
            checkResultSet = checkStatement.executeQuery();

            // try to discover size of DATA column and binary-ness of ID
            ResultSetMetaData met = checkResultSet.getMetaData();
            obtainFlagsFromResultSetMeta(met, tmd);

            if (col == Collection.NODES) {
                String tableInfo = RDBJDBCTools.dumpResultSetMeta(met);
                diagnostics.append(tableInfo);
                String indexInfo = dumpIndexData(con.getMetaData(), met, tableName);
                if (!indexInfo.isEmpty()) {
                    diagnostics.append(" ").append(indexInfo);
                }
            }
            tablesPresent.add(tableName);
        } catch (SQLException ex) {
            // table does not appear to exist
            con.rollback();

            try {
                creatStatement = con.createStatement();
                creatStatement.execute(this.db.getTableCreationStatement(tableName));
                creatStatement.close();

                for (String ic : this.db.getIndexCreationStatements(tableName)) {
                    creatStatement = con.createStatement();
                    creatStatement.execute(ic);
                    creatStatement.close();
                }

                con.commit();

                tablesCreated.add(tableName);

                checkStatement2 = con.prepareStatement("select * from " + tableName + " where ID = ?");
                checkStatement2.setString(1, "0:/");
                ResultSet rs = checkStatement2.executeQuery();
                // try to discover size of DATA column and binary-ness of ID
                ResultSetMetaData met = rs.getMetaData();
                obtainFlagsFromResultSetMeta(met, tmd);

                if (col == Collection.NODES) {
                    String tableInfo = RDBJDBCTools.dumpResultSetMeta(met);
                    diagnostics.append(tableInfo);
                    String indexInfo = dumpIndexData(con.getMetaData(), met, tableName);
                    if (!indexInfo.isEmpty()) {
                        diagnostics.append(" ").append(indexInfo);
                    }
                }
            }
            catch (SQLException ex2) {
                LOG.error("Failed to create table " + tableName + " in " + dbname, ex2);
                throw ex2;
            }
        }
        finally {
            closeResultSet(checkResultSet);
            closeStatement(checkStatement);
            closeStatement(checkStatement2);
            closeStatement(creatStatement);
        }
    }

    @Override
    protected void finalize() {
        if (!this.ch.isClosed() && this.callStack != null) {
            LOG.debug("finalizing RDBDocumentStore that was not disposed", this.callStack);
        }
    }

    private <T extends Document> T readDocumentCached(final Collection<T> collection, final String id, int maxCacheAge) {
        if (collection != Collection.NODES) {
            return readDocumentUncached(collection, id, null);
        } else {
            CacheValue cacheKey = new StringValue(id);
            NodeDocument doc = null;
            if (maxCacheAge > 0) {
                // first try without lock
                doc = nodesCache.getIfPresent(cacheKey);
                if (doc != null) {
                    long lastCheckTime = doc.getLastCheckTime();
                    if (lastCheckTime != 0) {
                        if (maxCacheAge == Integer.MAX_VALUE || System.currentTimeMillis() - lastCheckTime < maxCacheAge) {
                            return castAsT(unwrap(doc));
                        }
                    }
                }
            }
            try {
                Lock lock = getAndLock(id);
                try {
                    // caller really wants the cache to be cleared
                    if (maxCacheAge == 0) {
                        invalidateNodesCache(id, true);
                        doc = null;
                    }
                    final NodeDocument cachedDoc = doc;
                    doc = nodesCache.get(cacheKey, new Callable<NodeDocument>() {
                        @Override
                        public NodeDocument call() throws Exception {
                            NodeDocument doc = (NodeDocument) readDocumentUncached(collection, id, cachedDoc);
                            if (doc != null) {
                                doc.seal();
                            }
                            return wrap(doc);
                        }
                    });
                    // inspect the doc whether it can be used
                    long lastCheckTime = doc.getLastCheckTime();
                    if (lastCheckTime != 0 && (maxCacheAge == 0 || maxCacheAge == Integer.MAX_VALUE)) {
                        // we either just cleared the cache or the caller does
                        // not care;
                    } else if (lastCheckTime != 0 && (System.currentTimeMillis() - lastCheckTime < maxCacheAge)) {
                        // is new enough
                    } else {
                        // need to at least revalidate
                        NodeDocument ndoc = (NodeDocument) readDocumentUncached(collection, id, cachedDoc);
                        if (ndoc != null) {
                            ndoc.seal();
                        }
                        doc = wrap(ndoc);
                        nodesCache.put(cacheKey, doc);
                    }
                } finally {
                    lock.unlock();
                }
                return castAsT(unwrap(doc));
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to load document with " + id, e);
            }
        }
    }

    @CheckForNull
    private <T extends Document> boolean internalCreate(Collection<T> collection, List<UpdateOp> updates) {
        try {
            boolean success = true;
            // try up to CHUNKSIZE ops in one transaction
            for (List<UpdateOp> chunks : Lists.partition(updates, CHUNKSIZE)) {
                List<T> docs = new ArrayList<T>();
                for (UpdateOp update : chunks) {
                    T doc = collection.newDocument(this);
                    update.increment(MODCOUNT, 1);
                    if (hasChangesToCollisions(update)) {
                        update.increment(COLLISIONSMODCOUNT, 1);
                    }
                    UpdateUtils.applyChanges(doc, update, comparator);
                    if (!update.getId().equals(doc.getId())) {
                        throw new DocumentStoreException("ID mismatch - UpdateOp: " + update.getId() + ", ID property: "
                                + doc.getId());
                    }
                    docs.add(doc);
                }
                boolean done = insertDocuments(collection, docs);
                if (done) {
                    for (T doc : docs) {
                        addToCache(collection, doc);
                    }
                }
                else {
                    success = false;
                }
            }
            return success;
        } catch (DocumentStoreException ex) {
            return false;
        }
    }

    @CheckForNull
    private <T extends Document> T internalCreateOrUpdate(Collection<T> collection, UpdateOp update, boolean allowCreate,
            boolean checkConditions) {
        T oldDoc = readDocumentCached(collection, update.getId(), Integer.MAX_VALUE);

        if (oldDoc == null) {
            if (!allowCreate) {
                return null;
            } else if (!update.isNew()) {
                throw new DocumentStoreException("Document does not exist: " + update.getId());
            }
            T doc = collection.newDocument(this);
            if (checkConditions && !checkConditions(doc, update.getConditions())) {
                return null;
            }
            update.increment(MODCOUNT, 1);
            if (hasChangesToCollisions(update)) {
                update.increment(COLLISIONSMODCOUNT, 1);
            }
            UpdateUtils.applyChanges(doc, update, comparator);
            try {
                insertDocuments(collection, Collections.singletonList(doc));
                addToCache(collection, doc);
                return oldDoc;
            } catch (DocumentStoreException ex) {
                // may have failed due to a race condition; try update instead
                // this is an edge case, so it's ok to bypass the cache
                // (avoiding a race condition where the DB is already updated
                // but the cache is not)
                oldDoc = readDocumentUncached(collection, update.getId(), null);
                if (oldDoc == null) {
                    // something else went wrong
                    LOG.error("insert failed, but document " + update.getId() + " is not present, aborting", ex);
                    throw (ex);
                }
                return internalUpdate(collection, update, oldDoc, checkConditions, RETRIES);
            }
        } else {
            T result = internalUpdate(collection, update, oldDoc, checkConditions, RETRIES);
            if (allowCreate && result == null) {
                // TODO OAK-2655 need to implement some kind of retry
                LOG.error("update of " + update.getId() + " failed, race condition?");
                throw new DocumentStoreException("update of " + update.getId() + " failed, race condition?");
            }
            return result;
        }
    }

    /**
     * @return previous version of document or <code>null</code>
     */
    @CheckForNull
    private <T extends Document> T internalUpdate(Collection<T> collection, UpdateOp update, T oldDoc, boolean checkConditions,
            int maxRetries) {
        T doc = applyChanges(collection, oldDoc, update, checkConditions);
        if (doc == null) {
            // conditions not met
            return null;
        } else {
            Lock l = getAndLock(update.getId());
            try {
                boolean success = false;

                int retries = maxRetries;
                while (!success && retries > 0) {
                    long lastmodcount = modcountOf(oldDoc);
                    success = updateDocument(collection, doc, update, lastmodcount);
                    if (!success) {
                        retries -= 1;
                        oldDoc = readDocumentCached(collection, update.getId(), Integer.MAX_VALUE);
                        if (oldDoc != null) {
                            long newmodcount = modcountOf(oldDoc);
                            if (lastmodcount == newmodcount) {
                                // cached copy did not change so it probably was
                                // updated by a different instance, get a fresh one
                                oldDoc = readDocumentUncached(collection, update.getId(), null);
                            }
                        }

                        if (oldDoc == null) {
                            // document was there but is now gone
                            LOG.debug("failed to apply update because document is gone in the meantime: " + update.getId(), new Exception("call stack"));
                            return null;
                        }

                        doc = applyChanges(collection, oldDoc, update, checkConditions);
                        if (doc == null) {
                            return null;
                        }
                    } else {
                        if (collection == Collection.NODES) {
                            applyToCache((NodeDocument) oldDoc, (NodeDocument) doc);
                        }
                    }
                }

                if (!success) {
                    throw new DocumentStoreException("failed update of " + doc.getId() + " (race?) after " + maxRetries
                            + " retries");
                }

                return oldDoc;
            } finally {
                l.unlock();
            }
        }
    }

    @CheckForNull
    private <T extends Document> T applyChanges(Collection<T> collection, T oldDoc, UpdateOp update, boolean checkConditions) {
        T doc = collection.newDocument(this);
        oldDoc.deepCopy(doc);
        if (checkConditions && !checkConditions(doc, update.getConditions())) {
            return null;
        }
        if (hasChangesToCollisions(update)) {
            update.increment(COLLISIONSMODCOUNT, 1);
        }
        update.increment(MODCOUNT, 1);
        UpdateUtils.applyChanges(doc, update, comparator);
        doc.seal();
        return doc;
    }

    @CheckForNull
    private <T extends Document> void internalUpdate(Collection<T> collection, List<String> ids, UpdateOp update) {

        if (isAppendableUpdate(update) && !requiresPreviousState(update)) {
            Operation modOperation = update.getChanges().get(MODIFIEDKEY);
            long modified = getModifiedFromOperation(modOperation);
            boolean modifiedIsConditional = modOperation == null || modOperation.type != UpdateOp.Operation.Type.SET;
            String appendData = SR.asString(update);

            for (List<String> chunkedIds : Lists.partition(ids, CHUNKSIZE)) {
                // remember what we already have in the cache
                Set<QueryContext> seenQueryContext = new HashSet<QueryContext>();
                Map<String, NodeDocument> cachedDocs = Collections.emptyMap();
                // keep concurrently running queries from updating
                // the cache entry for this key
                for (QueryContext qc : qmap.values()) {
                    qc.addKeys(chunkedIds);
                    seenQueryContext.add(qc);
                }
                if (collection == Collection.NODES) {
                    cachedDocs = new HashMap<String, NodeDocument>();
                    for (String key : chunkedIds) {
                        cachedDocs.put(key, nodesCache.getIfPresent(new StringValue(key)));
                    }
                }

                Connection connection = null;
                TableMetaData tmd = getTable(collection);
                boolean success = false;
                try {
                    connection = this.ch.getRWConnection();
                    success = dbBatchedAppendingUpdate(connection, tmd, chunkedIds, modified, modifiedIsConditional, appendData);
                    connection.commit();
                } catch (SQLException ex) {
                    success = false;
                    this.ch.rollbackConnection(connection);
                } finally {
                    this.ch.closeConnection(connection);
                }
                if (success) {
                    // keep concurrently running queries from updating
                    // the cache entry for this key
                    for (QueryContext qc : qmap.values()) {
                        if (!seenQueryContext.contains(qc)) {
                            qc.addKeys(chunkedIds);
                        }
                    }
                    for (Entry<String, NodeDocument> entry : cachedDocs.entrySet()) {
                        T oldDoc = castAsT(entry.getValue());
                        if (oldDoc == null) {
                            String id = entry.getKey();
                            // make sure concurrently loaded document is
                            // invalidated
                            nodesCache.invalidate(new StringValue(id));
                        } else {
                            T newDoc = applyChanges(collection, oldDoc, update, true);
                            if (newDoc != null) {
                                applyToCache((NodeDocument) oldDoc, (NodeDocument) newDoc);
                            }
                        }
                    }
                } else {
                    for (String id : chunkedIds) {
                        UpdateOp up = update.copy();
                        up = up.shallowCopy(id);
                        internalCreateOrUpdate(collection, up, false, true);
                    }
                }
            }
        } else {
            for (String id : ids) {
                UpdateOp up = update.copy();
                up = up.shallowCopy(id);
                internalCreateOrUpdate(collection, up, false, true);
            }
        }
    }

    /**
     * Class used to track which documents may have been updated since the start
     * of the query and thus may not put into the cache.
     */
    private class QueryContext {

        private static final double FPP = 0.01d;
        private static final int ENTRIES_SCOPED = 1000;
        private static final int ENTRIES_OPEN = 10000;

        private final String fromKey, toKey;
        private volatile BloomFilter<String> filter = null;

        private BloomFilter<String> getFilter() {
            if (filter == null) {
                synchronized (this) {
                    if (filter == null) {
                        filter = BloomFilter.create(new Funnel<String>() {
                            private static final long serialVersionUID = -7114267990225941161L;

                            @Override
                            public void funnel(String from, PrimitiveSink into) {
                                into.putUnencodedChars(from);
                            }
                        }, toKey.equals(NodeDocument.MAX_ID_VALUE) ? ENTRIES_OPEN : ENTRIES_SCOPED, FPP);
                    }
                }
            }
            return filter;
        }

        public QueryContext(String fromKey, String toKey) {
            this.fromKey = fromKey;
            this.toKey = toKey;
        }

        public void addKey(String key) {
            if (fromKey.compareTo(key) < 0 && toKey.compareTo(key) > 0) {
                getFilter().put(key);
            }
        }

        public void addKeys(List<String> keys) {
            for (String key: keys) {
                addKey(key);
            }
        }

        public boolean mayUpdate(String key) {
            return filter == null ? true : !getFilter().mightContain(key);
        }

        synchronized public void dispose() {
            if (LOG.isDebugEnabled()) {
                if (filter != null) {
                    LOG.debug("Disposing QueryContext for range " + fromKey + "..." + toKey + " - filter fpp was: "
                            + filter.expectedFpp());
                } else {
                    LOG.debug("Disposing QueryContext for range " + fromKey + "..." + toKey + " - no filter was needed");
                }
            }
        }
    }

    private Map<Thread, QueryContext> qmap = new ConcurrentHashMap<Thread, QueryContext>();

    private <T extends Document> List<T> internalQuery(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {
        Connection connection = null;
        TableMetaData tmd = getTable(collection);
        if (indexedProperty != null && (!INDEXEDPROPERTIES.contains(indexedProperty))) {
            String message = "indexed property " + indexedProperty + " not supported, query was '>= '" + startValue
                    + "'; supported properties are " + INDEXEDPROPERTIES;
            LOG.info(message);
            throw new DocumentStoreException(message);
        }
        try {
            long now = System.currentTimeMillis();
            QueryContext qp = new QueryContext(fromKey, toKey);
            qmap.put(Thread.currentThread(), qp);
            connection = this.ch.getROConnection();
            String from = collection == Collection.NODES && NodeDocument.MIN_ID_VALUE.equals(fromKey) ? null : fromKey;
            String to = collection == Collection.NODES && NodeDocument.MAX_ID_VALUE.equals(toKey) ? null : toKey;
            List<RDBRow> dbresult = dbQuery(connection, tmd, from, to, indexedProperty, startValue, limit);
            connection.commit();

            int size = dbresult.size();
            List<T> result = new ArrayList<T>(size);
            for (int i = 0; i < size; i++) {
                RDBRow row = dbresult.set(i, null); // free RDBRow ASAP
                T doc = runThroughCache(collection, row, now, qp);
                result.add(doc);
            }
            qp.dispose();
            return result;
        } catch (Exception ex) {
            LOG.error("SQL exception on query", ex);
            throw new DocumentStoreException(ex);
        } finally {
            qmap.remove(Thread.currentThread());
            this.ch.closeConnection(connection);
        }
    }

    @Nonnull
    private <T extends Document> TableMetaData getTable(Collection<T> collection) {
        TableMetaData tmd = this.tableMeta.get(collection);
        if (tmd != null) {
            return tmd;
        } else {
            throw new IllegalArgumentException("Unknown collection: " + collection.toString());
        }
    }

    @CheckForNull
    private <T extends Document> T readDocumentUncached(Collection<T> collection, String id, NodeDocument cachedDoc) {
        Connection connection = null;
        TableMetaData tmd = getTable(collection);
        try {
            long lastmodcount = -1;
            if (cachedDoc != null) {
                lastmodcount = modcountOf(cachedDoc);
            }
            connection = this.ch.getROConnection();
            RDBRow row = dbRead(connection, tmd, id, lastmodcount);
            connection.commit();
            if (row == null) {
                return null;
            } else {
                if (lastmodcount == row.getModcount()) {
                    // we can re-use the cached document
                    cachedDoc.markUpToDate(System.currentTimeMillis());
                    return castAsT(cachedDoc);
                } else {
                    return convertFromDBObject(collection, row);
                }
            }
        } catch (Exception ex) {
            throw new DocumentStoreException(ex);
        } finally {
            this.ch.closeConnection(connection);
        }
    }

    private <T extends Document> void delete(Collection<T> collection, String id) {
        Connection connection = null;
        TableMetaData tmd = getTable(collection);
        try {
            connection = this.ch.getRWConnection();
            dbDelete(connection, tmd, Collections.singletonList(id));
            connection.commit();
        } catch (Exception ex) {
            throw new DocumentStoreException(ex);
        } finally {
            this.ch.closeConnection(connection);
        }
    }

    private <T extends Document> int delete(Collection<T> collection, List<String> ids) {
        int numDeleted = 0;
        TableMetaData tmd = getTable(collection);
        for (List<String> sublist : Lists.partition(ids, 64)) {
            Connection connection = null;
            try {
                connection = this.ch.getRWConnection();
                numDeleted += dbDelete(connection, tmd, sublist);
                connection.commit();
            } catch (Exception ex) {
                throw new DocumentStoreException(ex);
            } finally {
                this.ch.closeConnection(connection);
            }
        }
        return numDeleted;
    }

    private <T extends Document> int delete(Collection<T> collection,
                                            Map<String, Map<Key, Condition>> toRemove) {
        int numDeleted = 0;
        TableMetaData tmd = getTable(collection);
        Map<String, Map<Key, Condition>> subMap = Maps.newHashMap();
        Iterator<Entry<String, Map<Key, Condition>>> it = toRemove.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Map<Key, Condition>> entry = it.next();
            subMap.put(entry.getKey(), entry.getValue());
            if (subMap.size() == 64 || !it.hasNext()) {
                Connection connection = null;
                try {
                    connection = this.ch.getRWConnection();
                    numDeleted += dbDelete(connection, tmd, subMap);
                    connection.commit();
                } catch (Exception ex) {
                    throw DocumentStoreException.convert(ex);
                } finally {
                    this.ch.closeConnection(connection);
                }
                subMap.clear();
            }
        }
        return numDeleted;
    }

    private <T extends Document> boolean updateDocument(@Nonnull Collection<T> collection, @Nonnull T document,
            @Nonnull UpdateOp update, Long oldmodcount) {
        Connection connection = null;
        TableMetaData tmd = getTable(collection);
        String data = null;
        try {
            connection = this.ch.getRWConnection();
            Operation modOperation = update.getChanges().get(MODIFIEDKEY);
            long modified = getModifiedFromOperation(modOperation);
            boolean modifiedIsConditional = modOperation == null || modOperation.type != UpdateOp.Operation.Type.SET;
            Number flagB = (Number) document.get(NodeDocument.HAS_BINARY_FLAG);
            Boolean hasBinary = flagB != null && flagB.intValue() == NodeDocument.HAS_BINARY_VAL;
            Boolean flagD = (Boolean) document.get(NodeDocument.DELETED_ONCE);
            Boolean deletedOnce = flagD != null && flagD.booleanValue();
            Long modcount = (Long) document.get(MODCOUNT);
            Long cmodcount = (Long) document.get(COLLISIONSMODCOUNT);
            boolean success = false;
            boolean shouldRetry = true;

            // every 16th update is a full rewrite
            if (isAppendableUpdate(update) && modcount % 16 != 0) {
                String appendData = SR.asString(update);
                if (appendData.length() < tmd.getDataLimitInOctets() / CHAR2OCTETRATIO) {
                    try {
                        success = dbAppendingUpdate(connection, tmd, document.getId(), modified, modifiedIsConditional, hasBinary,
                                deletedOnce, modcount, cmodcount, oldmodcount, appendData);
                        // if we get here, a retry is not going to help (the SQL
                        // operation succeeded but simply did not select a row
                        // that could be updated, likely because of the check on
                        // MODCOUNT
                        shouldRetry = false;
                        connection.commit();
                    } catch (SQLException ex) {
                        continueIfStringOverflow(ex);
                        this.ch.rollbackConnection(connection);
                        success = false;
                    }
                }
            }
            if (!success && shouldRetry) {
                data = SR.asString(document);
                success = dbUpdate(connection, tmd, document.getId(), modified, hasBinary, deletedOnce, modcount, cmodcount,
                        oldmodcount, data);
                connection.commit();
            }
            return success;
        } catch (SQLException ex) {
            this.ch.rollbackConnection(connection);
            String addDiags = "";
            if (RDBJDBCTools.matchesSQLState(ex, "22", "72")) {
                byte[] bytes = asBytes(data);
                addDiags = String.format(" (DATA size in Java characters: %d, in octets: %d, computed character limit: %d)",
                        data.length(), bytes.length, tmd.getDataLimitInOctets() / CHAR2OCTETRATIO);
            }
            String message = String.format("Update for %s failed%s", document.getId(), addDiags);
            LOG.debug(message, ex);
            throw new DocumentStoreException(message, ex);
        } finally {
            this.ch.closeConnection(connection);
        }
    }

    private static void continueIfStringOverflow(SQLException ex) throws SQLException {
        String state = ex.getSQLState();
        if ("22001".equals(state) /* everybody */|| ("72000".equals(state) && 1489 == ex.getErrorCode()) /* Oracle */) {
            // ok
        } else {
            throw (ex);
        }
    }

    /*
     * currently we use append for all updates, but this might change in the
     * future
     */
    private static boolean isAppendableUpdate(UpdateOp update) {
        return true;
    }

    /*
     * check whether this update operation requires knowledge about the previous
     * state
     */
    private static boolean requiresPreviousState(UpdateOp update) {
        return !update.getConditions().isEmpty();
    }

    private static long getModifiedFromOperation(Operation op) {
        return op == null ? 0L : Long.parseLong(op.value.toString());
    }

    private <T extends Document> boolean insertDocuments(Collection<T> collection, List<T> documents) {
        Connection connection = null;
        TableMetaData tmd = getTable(collection);
        try {
            connection = this.ch.getRWConnection();
            boolean result = dbInsert(connection, tmd, documents);
            connection.commit();
            return result;
        } catch (SQLException ex) {
            this.ch.rollbackConnection(connection);

            List<String> ids = new ArrayList<String>();
            for (T doc : documents) {
                ids.add(doc.getId());
            }
            String message = String.format("insert of %s failed", ids);
            LOG.debug(message, ex);

            // collect additional exceptions
            String messages = LOG.isDebugEnabled() ? RDBJDBCTools.getAdditionalMessages(ex) : "";

            // see whether a DATA error was involved
            boolean dataRelated = false;
            SQLException walk = ex;
            while (walk != null && !dataRelated) {
                dataRelated = RDBJDBCTools.matchesSQLState(walk, "22", "72");
                walk = walk.getNextException();
            }
            if (dataRelated) {
                String id = null;
                int longest = 0, longestChars = 0;

                for (Document d : documents) {
                    String data = SR.asString(d);
                    byte bytes[] = asBytes(data);
                    if (bytes.length > longest) {
                        longest = bytes.length;
                        longestChars = data.length();
                        id = d.getId();
                    }
                }

                String m = String
                        .format(" (potential cause: long data for ID %s - longest octet DATA size in Java characters: %d, in octets: %d, computed character limit: %d)",
                                id, longest, longestChars, tmd.getDataLimitInOctets() / CHAR2OCTETRATIO);
                messages += m;
            }

            if (!messages.isEmpty()) {
                LOG.debug("additional diagnostics: " + messages);
            }

            throw new DocumentStoreException(message, ex);
        } finally {
            this.ch.closeConnection(connection);
        }
    }

    // configuration

    // Whether to use GZIP compression
    private static final boolean NOGZIP = Boolean
            .getBoolean("org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.NOGZIP");
    // Number of documents to insert at once for batch create
    private static final int CHUNKSIZE = Integer.getInteger(
            "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.CHUNKSIZE", 64);
    // Number of query hits above which a diagnostic warning is generated
    private static final int QUERYHITSLIMIT = Integer.getInteger(
            "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QUERYHITSLIMIT", 4096);
    // Number of elapsed ms in a query above which a diagnostic warning is generated
    private static final int QUERYTIMELIMIT = Integer.getInteger(
            "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QUERYTIMELIMIT", 10000);

    private static byte[] asBytes(String data) {
        byte[] bytes;
        try {
            bytes = data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            LOG.error("UTF-8 not supported??", ex);
            throw new DocumentStoreException(ex);
        }

        if (NOGZIP) {
            return bytes;
        } else {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length());
                GZIPOutputStream gos = new GZIPOutputStream(bos) {
                    {
                        // TODO: make this configurable
                        this.def.setLevel(Deflater.BEST_SPEED);
                    }
                };
                gos.write(bytes);
                gos.close();
                return bos.toByteArray();
            } catch (IOException ex) {
                LOG.error("Error while gzipping contents", ex);
                throw new DocumentStoreException(ex);
            }
        }
    }

    private static void setIdInStatement(TableMetaData tmd, PreparedStatement stmt, int idx, String id) throws SQLException {
        if (tmd.isIdBinary()) {
            try {
                stmt.setBytes(idx, id.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                LOG.error("UTF-8 not supported??", ex);
                throw new DocumentStoreException(ex);
            }
        } else {
            stmt.setString(idx, id);
        }
    }

    private static String getIdFromRS(TableMetaData tmd, ResultSet rs, int idx) throws SQLException {
        if (tmd.isIdBinary()) {
            try {
                return new String(rs.getBytes(idx), "UTF-8");
            } catch (UnsupportedEncodingException ex) {
                LOG.error("UTF-8 not supported??", ex);
                throw new DocumentStoreException(ex);
            }
        } else {
            return rs.getString(idx);
        }
    }

    @CheckForNull
    private RDBRow dbRead(Connection connection, TableMetaData tmd, String id, long lastmodcount) throws SQLException {
        PreparedStatement stmt;

        boolean useCaseStatement = lastmodcount != -1 && this.db.allowsCaseInSelect();
        if (useCaseStatement) {
            // the case statement causes the actual row data not to be
            // sent in case we already have it
            stmt = connection
                    .prepareStatement("select MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, case MODCOUNT when ? then null else DATA end as DATA, "
                            + "case MODCOUNT when ? then null else BDATA end as BDATA from " + tmd.getName() + " where ID = ?");
        } else {
            // either we don't have a previous version of the document
            // or the database does not support CASE in SELECT
            stmt = connection.prepareStatement("select MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA from "
                    + tmd.getName() + " where ID = ?");
        }

        try {
            int si = 1;
            if (useCaseStatement) {
                stmt.setLong(si++, lastmodcount);
                stmt.setLong(si++, lastmodcount);
            }
            setIdInStatement(tmd, stmt, si, id);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long modified = rs.getLong(1);
                long modcount = rs.getLong(2);
                long cmodcount = rs.getLong(3);
                long hasBinary = rs.getLong(4);
                long deletedOnce = rs.getLong(5);
                String data = rs.getString(6);
                byte[] bdata = rs.getBytes(7);
                return new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            LOG.error("attempting to read " + id + " (id length is " + id.length() + ")", ex);
            // DB2 throws an SQLException for invalid keys; handle this more
            // gracefully
            if ("22001".equals(ex.getSQLState())) {
                this.ch.rollbackConnection(connection);
                return null;
            } else {
                throw (ex);
            }
        } finally {
            stmt.close();
        }
    }

    private List<RDBRow> dbQuery(Connection connection, TableMetaData tmd, String minId, String maxId, String indexedProperty,
            long startValue, int limit) throws SQLException {
        long start = System.currentTimeMillis();
        StringBuilder selectClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();
        if (limit != Integer.MAX_VALUE && this.db.getFetchFirstSyntax() == FETCHFIRSTSYNTAX.TOP) {
            selectClause.append("TOP " + limit +  " ");
        }
        selectClause.append("ID, MODIFIED, MODCOUNT, CMODCOUNT, HASBINARY, DELETEDONCE, DATA, BDATA from ").append(tmd.getName());

        // dynamically build where clause
        String whereSep = "";
        if (minId != null) {
            whereClause.append("ID > ?");
            whereSep = " and ";
        }
        if (maxId != null) {
            whereClause.append(whereSep).append("ID < ?");
            whereSep = " and ";
        }

        if (indexedProperty != null) {
            if (MODIFIED.equals(indexedProperty)) {
                whereClause.append(whereSep).append("MODIFIED >= ?");
            } else if (NodeDocument.HAS_BINARY_FLAG.equals(indexedProperty)) {
                if (startValue != NodeDocument.HAS_BINARY_VAL) {
                    throw new DocumentStoreException("unsupported value for property " + NodeDocument.HAS_BINARY_FLAG);
                }
                whereClause.append(whereSep).append("HASBINARY = 1");
            } else if (NodeDocument.DELETED_ONCE.equals(indexedProperty)) {
                if (startValue != 1) {
                    throw new DocumentStoreException("unsupported value for property " + NodeDocument.DELETED_ONCE);
                }
                whereClause.append(whereSep).append("DELETEDONCE = 1");
            } else {
                throw new DocumentStoreException("unsupported indexed property: " + indexedProperty);
            }
        }

        StringBuilder query = new StringBuilder();
        query.append("select ").append(selectClause);
        if (whereClause.length() != 0) {
            query.append(" where ").append(whereClause);
        }

        query.append(" order by ID");

        if (limit != Integer.MAX_VALUE) {
            switch (this.db.getFetchFirstSyntax()) {
                case LIMIT:
                    query.append(" LIMIT " + limit);
                    break;
                case FETCHFIRST:
                    query.append(" FETCH FIRST " + limit + " ROWS ONLY");
                    break;
                default:
                    break;
            }
        }

        PreparedStatement stmt = connection.prepareStatement(query.toString());
        List<RDBRow> result = new ArrayList<RDBRow>();
        long dataTotal = 0, bdataTotal = 0;
        try {
            int si = 1;
            if (minId != null) {
                setIdInStatement(tmd, stmt, si++, minId);
            }
            if (maxId != null) {
                setIdInStatement(tmd, stmt, si++, maxId);
            }

            if (MODIFIED.equals(indexedProperty)) {
                stmt.setLong(si++, startValue);
            }
            if (limit != Integer.MAX_VALUE) {
                stmt.setFetchSize(limit);
            }
            ResultSet rs = stmt.executeQuery();
            while (rs.next() && result.size() < limit) {
                String id = getIdFromRS(tmd, rs, 1);

                if ((minId != null && id.compareTo(minId) < 0) || (maxId != null && id.compareTo(maxId) > 0)) {
                    throw new DocumentStoreException(
                            "unexpected query result: '" + minId + "' < '" + id + "' < '" + maxId + "' - broken DB collation?");
                }
                long modified = rs.getLong(2);
                long modcount = rs.getLong(3);
                long cmodcount = rs.getLong(4);
                long hasBinary = rs.getLong(5);
                long deletedOnce = rs.getLong(6);
                String data = rs.getString(7);
                byte[] bdata = rs.getBytes(8);
                result.add(new RDBRow(id, hasBinary == 1, deletedOnce == 1, modified, modcount, cmodcount, data, bdata));
                dataTotal += data.length();
                bdataTotal += bdata == null ? 0 : bdata.length;
            }
        } finally {
            stmt.close();
        }

        long elapsed = System.currentTimeMillis() - start;
        if (QUERYHITSLIMIT != 0 && result.size() > QUERYHITSLIMIT) {
            String message = String.format("Potentially excessive query with %d hits (limited to %d, configured QUERYHITSLIMIT %d), elapsed time %dms, params minid '%s' maxid '%s' indexedProperty %s startValue %d limit %d. Check calling method.",
                    result.size(), limit, QUERYHITSLIMIT, elapsed, minId, maxId, indexedProperty, startValue, limit);
            LOG.info(message, new Exception("call stack"));
        }
        else if (QUERYTIMELIMIT != 0 && elapsed > QUERYTIMELIMIT) {
            String message = String.format("Long running query with %d hits (limited to %d), elapsed time %dms (configured QUERYTIMELIMIT %d), params minid '%s' maxid '%s' indexedProperty %s startValue %d limit %d. Read %d chars from DATA and %d bytes from BDATA. Check calling method.",
                    result.size(), limit, elapsed, QUERYTIMELIMIT, minId, maxId, indexedProperty, startValue, limit, dataTotal, bdataTotal);
            LOG.info(message, new Exception("call stack"));
        }

        return result;
    }

    private boolean dbUpdate(Connection connection, TableMetaData tmd, String id, Long modified, Boolean hasBinary,
            Boolean deletedOnce, Long modcount, Long cmodcount, Long oldmodcount, String data) throws SQLException {
        String t = "update "
                + tmd.getName()
                + " set MODIFIED = ?, HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, BDATA = ? where ID = ?";
        if (oldmodcount != null) {
            t += " and MODCOUNT = ?";
        }
        PreparedStatement stmt = connection.prepareStatement(t);
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            stmt.setObject(si++, hasBinary ? 1 : 0, Types.SMALLINT);
            stmt.setObject(si++, deletedOnce ? 1 : 0, Types.SMALLINT);
            stmt.setObject(si++, modcount, Types.BIGINT);
            stmt.setObject(si++, cmodcount == null ? Long.valueOf(0) : cmodcount, Types.BIGINT);
            stmt.setObject(si++, data.length(), Types.BIGINT);

            if (data.length() < tmd.getDataLimitInOctets() / CHAR2OCTETRATIO) {
                stmt.setString(si++, data);
                stmt.setBinaryStream(si++, null, 0);
            } else {
                stmt.setString(si++, "\"blob\"");
                byte[] bytes = asBytes(data);
                stmt.setBytes(si++, bytes);
            }

            setIdInStatement(tmd, stmt, si++, id);

            if (oldmodcount != null) {
                stmt.setObject(si++, oldmodcount, Types.BIGINT);
            }
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB update failed for " + tmd.getName() + "/" + id + " with oldmodcount=" + oldmodcount);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    private boolean dbAppendingUpdate(Connection connection, TableMetaData tmd, String id, Long modified,
            boolean setModifiedConditionally, Boolean hasBinary, Boolean deletedOnce, Long modcount, Long cmodcount,
            Long oldmodcount, String appendData) throws SQLException {
        StringBuilder t = new StringBuilder();
        t.append("update " + tmd.getName() + " set ");
        t.append(setModifiedConditionally ? "MODIFIED = case when ? > MODIFIED then ? else MODIFIED end, " : "MODIFIED = ?, ");
        t.append("HASBINARY = ?, DELETEDONCE = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = DSIZE + ?, ");
        t.append("DATA = " + this.db.getConcatQueryString(tmd.getDataLimitInOctets(), appendData.length()) + " ");
        t.append("where ID = ?");
        if (oldmodcount != null) {
            t.append(" and MODCOUNT = ?");
        }
        PreparedStatement stmt = connection.prepareStatement(t.toString());
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            if (setModifiedConditionally) {
                stmt.setObject(si++, modified, Types.BIGINT);
            }
            stmt.setObject(si++, hasBinary ? 1 : 0, Types.SMALLINT);
            stmt.setObject(si++, deletedOnce ? 1 : 0, Types.SMALLINT);
            stmt.setObject(si++, modcount, Types.BIGINT);
            stmt.setObject(si++, cmodcount == null ? Long.valueOf(0) : cmodcount, Types.BIGINT);
            stmt.setObject(si++, 1 + appendData.length(), Types.BIGINT);
            stmt.setString(si++, "," + appendData);
            setIdInStatement(tmd, stmt, si++, id);

            if (oldmodcount != null) {
                stmt.setObject(si++, oldmodcount, Types.BIGINT);
            }
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB append update failed for " + tmd.getName() + "/" + id + " with oldmodcount=" + oldmodcount);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    private boolean dbBatchedAppendingUpdate(Connection connection, TableMetaData tmd, List<String> ids, Long modified,
            boolean setModifiedConditionally,
            String appendData) throws SQLException {
        StringBuilder t = new StringBuilder();
        t.append("update " + tmd.getName() + " set ");
        t.append(setModifiedConditionally ? "MODIFIED = case when ? > MODIFIED then ? else MODIFIED end, " : "MODIFIED = ?, ");
        t.append("MODCOUNT = MODCOUNT + 1, DSIZE = DSIZE + ?, ");
        t.append("DATA = " + this.db.getConcatQueryString(tmd.getDataLimitInOctets(), appendData.length()) + " ");
        t.append("where ID in (");
        for (int i = 0; i < ids.size(); i++) {
            if (i != 0) {
                t.append(',');
            }
            t.append('?');
        }
        t.append(")");
        PreparedStatement stmt = connection.prepareStatement(t.toString());
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            if (setModifiedConditionally) {
                stmt.setObject(si++, modified, Types.BIGINT);
            }
            stmt.setObject(si++, 1 + appendData.length(), Types.BIGINT);
            stmt.setString(si++, "," + appendData);
            for (String id : ids) {
                setIdInStatement(tmd, stmt, si++, id);
            }
            int result = stmt.executeUpdate();
            if (result != ids.size()) {
                LOG.debug("DB update failed: only " + result + " of " + ids.size() + " updated. Table: " + tmd.getName() + ", IDs:"
                        + ids);
            }
            return result == ids.size();
        } finally {
            stmt.close();
        }
    }

    private <T extends Document> boolean dbInsert(Connection connection, TableMetaData tmd, List<T> documents) throws SQLException {

        PreparedStatement stmt = connection.prepareStatement("insert into " + tmd.getName() +
                "(ID, MODIFIED, HASBINARY, DELETEDONCE, MODCOUNT, CMODCOUNT, DSIZE, DATA, BDATA) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        try {
            for (T document : documents) {
                String data = SR.asString(document);
                String id = document.getId();
                Number hasBinary = (Number) document.get(NodeDocument.HAS_BINARY_FLAG);
                Boolean deletedOnce = (Boolean) document.get(NodeDocument.DELETED_ONCE);
                Long cmodcount = (Long) document.get(COLLISIONSMODCOUNT);

                int si = 1;
                setIdInStatement(tmd, stmt, si++, id);
                stmt.setObject(si++, document.get(MODIFIED), Types.BIGINT);
                stmt.setObject(si++, (hasBinary != null && hasBinary.intValue() == NodeDocument.HAS_BINARY_VAL) ? 1 : 0, Types.SMALLINT);
                stmt.setObject(si++, (deletedOnce != null && deletedOnce) ? 1 : 0, Types.SMALLINT);
                stmt.setObject(si++, document.get(MODCOUNT), Types.BIGINT);
                stmt.setObject(si++, cmodcount == null ? Long.valueOf(0) : cmodcount, Types.BIGINT);
                stmt.setObject(si++, data.length(), Types.BIGINT);
                if (data.length() < tmd.getDataLimitInOctets() / CHAR2OCTETRATIO) {
                    stmt.setString(si++, data);
                    stmt.setBinaryStream(si++, null, 0);
                } else {
                    stmt.setString(si++, "\"blob\"");
                    byte[] bytes = asBytes(data);
                    stmt.setBytes(si++, bytes);
                }
                stmt.addBatch();
            }
            int[] results = stmt.executeBatch();
            boolean success = true;
            for (int i = 0; i < documents.size(); i++) {
                int result = results[i];
                if (result != 1 && result != Statement.SUCCESS_NO_INFO) {
                    LOG.error("DB insert failed for {}: {}", tmd.getName(), documents.get(i).getId());
                    success = false;
                }
            }
            return success;
        } finally {
            stmt.close();
        }
    }

    private int dbDelete(Connection connection, TableMetaData tmd, List<String> ids) throws SQLException {

        PreparedStatement stmt;
        int cnt = ids.size();

        if (cnt == 1) {
            stmt = connection.prepareStatement("delete from " + tmd.getName() + " where ID=?");
        } else {
            StringBuilder inClause = new StringBuilder();
            for (int i = 0; i < cnt; i++) {
                inClause.append('?');
                if (i != cnt - 1) {
                    inClause.append(',');
                }
            }
            stmt = connection.prepareStatement("delete from " + tmd.getName() + " where ID in (" + inClause.toString() + ")");
        }

        try {
            for (int i = 0; i < cnt; i++) {
                setIdInStatement(tmd, stmt, i + 1, ids.get(i));
            }
            int result = stmt.executeUpdate();
            if (result != cnt) {
                LOG.debug("DB delete failed for " + tmd.getName() + "/" + ids);
            }
            return result;
        } finally {
            stmt.close();
        }
    }

    private int dbDelete(Connection connection, TableMetaData tmd,
                         Map<String, Map<Key, Condition>> toDelete)
            throws SQLException, DocumentStoreException {
        String or = "";
        StringBuilder whereClause = new StringBuilder();
        for (Entry<String, Map<Key, Condition>> entry : toDelete.entrySet()) {
            whereClause.append(or);
            or = " or ";
            whereClause.append("ID=?");
            for (Entry<Key, Condition> c : entry.getValue().entrySet()) {
                if (!c.getKey().getName().equals(MODIFIED)) {
                    throw new DocumentStoreException(
                            "Unsupported condition: " + c);
                }
                whereClause.append(" and MODIFIED");
                if (c.getValue().type == Condition.Type.EQUALS
                        && c.getValue().value instanceof Long) {
                    whereClause.append("=?");
                } else if (c.getValue().type == Condition.Type.EXISTS) {
                    whereClause.append(" is not null");
                } else {
                    throw new DocumentStoreException(
                            "Unsupported condition: " + c);
                }
            }
        }

        PreparedStatement stmt= connection.prepareStatement(
                "delete from " + tmd.getName() + " where " + whereClause);
        try {
            int i = 1;
            for (Entry<String, Map<Key, Condition>> entry : toDelete.entrySet()) {
                setIdInStatement(tmd, stmt, i++, entry.getKey());
                for (Entry<Key, Condition> c : entry.getValue().entrySet()) {
                    if (c.getValue().type == Condition.Type.EQUALS) {
                        stmt.setLong(i++, (Long) c.getValue().value);
                    }
                }
            }
            return stmt.executeUpdate();
        } finally {
            stmt.close();
        }
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        // ignored
    }

    @SuppressWarnings("unchecked")
    private static <T extends Document> T castAsT(NodeDocument doc) {
        return (T) doc;
    }

    // Memory Cache
    private Cache<CacheValue, NodeDocument> nodesCache;
    private CacheStats cacheStats;
    private final Striped<Lock> locks = Striped.lock(64);

    private Lock getAndLock(String key) {
        Lock l = locks.get(key);
        l.lock();
        return l;
    }

    @CheckForNull
    private static NodeDocument unwrap(@Nonnull NodeDocument doc) {
        return doc == NodeDocument.NULL ? null : doc;
    }

    @Nonnull
    private static NodeDocument wrap(@CheckForNull NodeDocument doc) {
        return doc == null ? NodeDocument.NULL : doc;
    }

    @Nonnull
    private static String idOf(@Nonnull Document doc) {
        String id = doc.getId();
        if (id == null) {
            throw new IllegalArgumentException("non-null ID expected");
        }
        return id;
    }

    private static long modcountOf(@Nonnull Document doc) {
        Number n = doc.getModCount();
        return n != null ? n.longValue() : -1;
    }

    /**
     * Adds a document to the {@link #nodesCache} iff there is no document in
     * the cache with the document key. This method does not acquire a lock from
     * {@link #locks}! The caller must ensure a lock is held for the given
     * document.
     * 
     * @param doc
     *            the document to add to the cache.
     * @return either the given <code>doc</code> or the document already present
     *         in the cache.
     */
    @Nonnull
    private NodeDocument addToCache(@Nonnull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();
        // make sure we only cache the document if it wasn't
        // changed and cached by some other thread in the
        // meantime. That is, use get() with a Callable,
        // which is only used when the document isn't there
        try {
            CacheValue key = new StringValue(idOf(doc));
            for (;;) {
                NodeDocument cached = nodesCache.get(key, new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() {
                        return doc;
                    }
                });
                if (cached != NodeDocument.NULL) {
                    return cached;
                } else {
                    nodesCache.invalidate(key);
                }
            }
        } catch (ExecutionException e) {
            // will never happen because call() just returns
            // the already available doc
            throw new IllegalStateException(e);
        }
    }

    @Nonnull
    private void applyToCache(@Nonnull final NodeDocument oldDoc, @Nonnull final NodeDocument newDoc) {
        NodeDocument cached = addToCache(newDoc);
        if (cached == newDoc) {
            // successful
            return;
        } else if (oldDoc == null) {
            // this is an insert and some other thread was quicker
            // loading it into the cache -> return now
            return;
        } else {
            CacheValue key = new StringValue(idOf(newDoc));
            // this is an update (oldDoc != null)
            if (Objects.equal(cached.getModCount(), oldDoc.getModCount())) {
                nodesCache.put(key, newDoc);
            } else {
                // the cache entry was modified by some other thread in
                // the meantime. the updated cache entry may or may not
                // include this update. we cannot just apply our update
                // on top of the cached entry.
                // therefore we must invalidate the cache entry
                nodesCache.invalidate(key);
            }
        }
    }

    private <T extends Document> void addToCache(Collection<T> collection, T doc) {
        if (collection == Collection.NODES) {
            Lock lock = getAndLock(idOf(doc));
            try {
                addToCache((NodeDocument) doc);
            } finally {
                lock.unlock();
            }
        }
    }

    @Nonnull
    protected <T extends Document> T convertFromDBObject(@Nonnull Collection<T> collection, @Nonnull RDBRow row) {
        // this method is present here in order to facilitate unit testing for OAK-3566
        return SR.fromRow(collection, row);
    }

    private <T extends Document> T runThroughCache(Collection<T> collection, RDBRow row, long now, QueryContext qp) {

        if (collection != Collection.NODES) {
            // not in the cache anyway
            return convertFromDBObject(collection, row);
        }

        String id = row.getId();
        CacheValue cacheKey = new StringValue(id);
        NodeDocument inCache = nodesCache.getIfPresent(cacheKey);
        Number modCount = row.getModcount();

        // do not overwrite document in cache if the
        // existing one in the cache is newer
        if (inCache != null && inCache != NodeDocument.NULL) {
            // check mod count
            Number cachedModCount = inCache.getModCount();
            if (cachedModCount == null) {
                throw new IllegalStateException("Missing " + Document.MOD_COUNT);
            }
            if (modCount.longValue() <= cachedModCount.longValue()) {
                // we can use the cached document
                inCache.markUpToDate(now);
                return castAsT(inCache);
            }
        }

        NodeDocument fresh = (NodeDocument) convertFromDBObject(collection, row);
        fresh.seal();

        if (!qp.mayUpdate(id)) {
            return castAsT(fresh);
        }

        Lock lock = getAndLock(id);
        try {
            inCache = nodesCache.getIfPresent(cacheKey);
            if (inCache != null && inCache != NodeDocument.NULL) {
                // check mod count
                Number cachedModCount = inCache.getModCount();
                if (cachedModCount == null) {
                    throw new IllegalStateException("Missing " + Document.MOD_COUNT);
                }
                if (modCount.longValue() > cachedModCount.longValue()) {
                    nodesCache.put(cacheKey, fresh);
                } else {
                    fresh = inCache;
                }
            } else {
                nodesCache.put(cacheKey, fresh);
            }
        } finally {
            lock.unlock();
        }
        return castAsT(fresh);
    }

    private boolean hasChangesToCollisions(UpdateOp update) {
        if (! USECMODCOUNT) return false;

        for (Entry<Key, Operation> e : checkNotNull(update).getChanges().entrySet()) {
            Key k = e.getKey();
            Operation op = e.getValue();
            if (op.type == Operation.Type.SET_MAP_ENTRY) {
                if (NodeDocument.COLLISIONS.equals(k.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    protected Cache<CacheValue, NodeDocument> getNodeDocumentCache() {
        return nodesCache;
    }
}
