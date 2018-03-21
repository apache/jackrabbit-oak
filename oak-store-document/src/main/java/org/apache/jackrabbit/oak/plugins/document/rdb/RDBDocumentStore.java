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
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.partition;
import static org.apache.jackrabbit.oak.plugins.document.UpdateUtils.checkConditions;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.asDocumentStoreException;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeResultSet;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeStatement;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.createTableName;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getModuleVersion;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreStatsCollector;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheChangesTracker;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.cache.ModificationStamp;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
 * <li>H2DB</li>
 * <li>Apache Derby</li>
 * <li>IBM DB2</li>
 * <li>PostgreSQL</li>
 * <li>MariaDB (MySQL)</li>
 * <li>Microsoft SQL Server</li>
 * <li>Oracle</li>
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
 * <table style="text-align: left;" summary="">
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
 * <td>The document's key (for databases that can not handle 512 character
 * primary keys, such as MySQL, varbinary is possible as well).</td>
 * </tr>
 * <tr>
 * <th>MODIFIED</th>
 * <td>bigint</td>
 * <td>Low-resolution timestamp.
 * </tr>
 * <tr>
 * <th>HASBINARY</th>
 * <td>smallint</td>
 * <td>Flag indicating whether the document has binary properties.
 * </tr>
 * <tr>
 * <th>DELETEDONCE</th>
 * <td>smallint</td>
 * <td>Flag indicating whether the document has been deleted once.
 * </tr>
 * <tr>
 * <th>MODCOUNT</th>
 * <td>bigint</td>
 * <td>Modification counter, used for avoiding overlapping updates.</td>
 * </tr>
 * <tr>
 * <th>DSIZE</th>
 * <td>bigint</td>
 * <td>The approximate size of the document's JSON serialization (for debugging
 * purposes).</td>
 * </tr>
 * <tr>
 * <th>VERSION</th>
 * <td>smallint</td>
 * <td>The schema version the code writing to a row (or inserting it) was aware
 * of (introduced with schema version 1). Not set for rows written by version 0
 * client code.</td>
 * </tr>
 * <tr>
 * <th>SDTYPE</th>
 * <td>smallint</td>
 * <td>Split Document type.</td>
 * </tr>
 * <tr>
 * <th>SDMAXREVTIME</th>
 * <td>bigint</td>
 * <td>Split document max revision time..</td>
 * </tr>
 * <tr>
 * <th>DATA</th>
 * <td>varchar(16384)</td>
 * <td>The document's JSON serialization (only used for small document sizes, in
 * which case BDATA (below) is not set), or a sequence of JSON serialized update
 * operations to be applied against the last full serialization.</td>
 * </tr>
 * <tr>
 * <th>BDATA</th>
 * <td>blob</td>
 * <td>The document's JSON serialization (usually GZIPped, only used for "large"
 * documents).</td>
 * </tr>
 * </tbody>
 * </table>
 * <p>
 * The names of database tables can be prefixed; the purpose is mainly for
 * testing, as tables can also be dropped automatically when the store is
 * disposed (this only happens for those tables that have been created on
 * demand).
 * <h4>Versioning</h4>
 * <p>
 * The initial database layout used in OAK 1.0 through 1.6 is version 0.
 * <p>
 * Version 1 introduces an additional "version" column, which records the schema
 * version of the code writing to the database (upon insert and update). This is
 * in preparation of future layout changes which might introduce new columns.
 * <p>
 * Version 2 introduces an additional "sdtype" and "sdmaxrevtime".
 * <p>
 * The code deals with both version 0, version 1 and version 2 table layouts. By
 * default, it tries to create version 2 tables, and also tries to upgrade
 * existing version 0 and 1 tables to version 2.
 * <h4>DB-specific information</h4>
 * <p>
 * <em>Note that the database needs to be created/configured to support all
 * Unicode characters in text fields, and to collate by Unicode code point (in
 * DB2: "collate using identity", in PostgreSQL: "C"). THIS IS NOT THE
 * DEFAULT!</em>
 * <p>
 * <em>For MySQL, the database parameter "max_allowed_packet" needs to be
 * increased to support ~16M blobs.</em>
 * 
 * <h3>Table Creation</h3>
 * <p>
 * The code tries to create the tables when they are not present. Likewise, it
 * tries to upgrade to a newer schema when needed.
 * <p>
 * Users/Administrators who prefer to stay in control over table generation can
 * create them "manually". {@link RDBHelper} can be used to print out the DDL
 * statements that would have been used for auto-creation.
 * 
 * <h3>Caching</h3>
 * <p>
 * The cache borrows heavily from the {@link MongoDocumentStore} implementation.
 * 
 * <h3>Queries</h3>
 * <p>
 * The implementation currently supports only three indexed properties: "_bin",
 * "deletedOnce", and "_modified". Attempts to use a different indexed property
 * will cause a {@link DocumentStoreException}.
 */
public class RDBDocumentStore implements DocumentStore {

    /**
     * Creates a {@linkplain RDBDocumentStore} instance using the provided
     * {@link DataSource}, {@link DocumentNodeStoreBuilder}, and {@link RDBOptions}.
     */
    public RDBDocumentStore(DataSource ds, DocumentNodeStoreBuilder<?> builder, RDBOptions options) {
        try {
            initialize(ds, builder, options);
        } catch (Exception ex) {
            throw asDocumentStoreException(ex, "initializing RDB document store");
        }
    }

    /**
     * Creates a {@linkplain RDBDocumentStore} instance using the provided
     * {@link DataSource}, {@link DocumentNodeStoreBuilder}, and default
     * {@link RDBOptions}.
     */
    public RDBDocumentStore(DataSource ds, DocumentNodeStoreBuilder<?> builder) {
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
        List<QueryCondition> conditions = Collections.emptyList();
        if (indexedProperty != null) {
            conditions = Collections.singletonList(new QueryCondition(indexedProperty, ">=", startValue));
        }
        return internalQuery(collection, fromKey, toKey, EMPTY_KEY_PATTERN, conditions, limit);
    }

    @Nonnull
    protected <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
            List<String> excludeKeyPatterns, List<QueryCondition> conditions, int limit) {
        return internalQuery(collection, fromKey, toKey, excludeKeyPatterns, conditions, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String id) {
        try {
            delete(collection, id);
        } finally {
            invalidateCache(collection, id, true);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> ids) {
        try {
            delete(collection, ids);
        } finally {
            for (String id : ids) {
                invalidateCache(collection, id, true);
            }
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Long> toRemove) {
        try {
            return delete(collection, toRemove);
        } finally {
            for (String id : toRemove.keySet()) {
                invalidateCache(collection, id, true);
            }
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException {
        try {
            List<QueryCondition> conditions = new ArrayList<QueryCondition>();
            conditions.add(new QueryCondition(indexedProperty, ">", startValue));
            conditions.add(new QueryCondition(indexedProperty, "<", endValue));
            return deleteWithCondition(collection, conditions);
        } finally {
            if (collection == Collection.NODES) {
                // this method is currently being used only for Journal
                // collection while GC. But, to keep sanctity of the API, we
                // need to acknowledge that Nodes collection could've been used.
                // But, in this signature, there's no useful way to invalidate
                // cache.
                // So, we use the hammer for this task
                invalidateCache();
            }
        }
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        return internalCreate(collection, updateOps);
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        UpdateUtils.assertUnconditional(update);
        return internalCreateOrUpdate(collection, update, true, false);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) {
        if (!BATCHUPDATES) {
            List<T> results = new ArrayList<T>(updateOps.size());
            for (UpdateOp update : updateOps) {
                results.add(createOrUpdate(collection, update));
            }
            return results;
        }

        final Stopwatch watch = startWatch();
        Map<UpdateOp, T> results = new LinkedHashMap<UpdateOp, T>();
        Map<String, UpdateOp> operationsToCover = new LinkedHashMap<String, UpdateOp>();
        Set<UpdateOp> duplicates = new HashSet<UpdateOp>();

        for (UpdateOp updateOp : updateOps) {
            UpdateUtils.assertUnconditional(updateOp);
            if (operationsToCover.containsKey(updateOp.getId())) {
                duplicates.add(updateOp);
                results.put(updateOp, null);
            } else {
                UpdateOp clone = updateOp.copy();
                addUpdateCounters(clone);
                operationsToCover.put(clone.getId(), clone);
                results.put(clone, null);
            }
        }

        Map<String, T> oldDocs = new HashMap<String, T>();
        if (collection == Collection.NODES) {
            oldDocs.putAll(readDocumentCached(collection, operationsToCover.keySet()));
        }

        int i = 0; // iteration count

        // bulk update requires two DB requests, so if we have <= 2 operations
        // it's better to send them sequentially
        while (operationsToCover.size() > 2) {
            // We should try to insert documents only during the first
            // iteration. In the 2nd and 3rd iterations we only deal with
            // conflicting documents, so they already exist in the database
            // and there's no point in inserting them.
            boolean upsert = i == 0;

            if (i++ == 3) {
                // operations that conflicted in 3 consecutive bulk
                // updates should be applied sequentially
                break;
            }

            for (List<UpdateOp> partition : partition(newArrayList(operationsToCover.values()), CHUNKSIZE)) {
                Map<UpdateOp, T> successfulUpdates = bulkUpdate(collection, partition, oldDocs, upsert);
                results.putAll(successfulUpdates);
                operationsToCover.values().removeAll(successfulUpdates.keySet());
            }
        }

        // if there are some changes left, we'll apply them one after another
        for (UpdateOp updateOp : updateOps) {
            UpdateOp conflictedOp = operationsToCover.remove(updateOp.getId());
            if (conflictedOp != null) {
                results.put(conflictedOp, createOrUpdate(collection, updateOp));
            } else if (duplicates.contains(updateOp)) {
                results.put(updateOp, createOrUpdate(collection, updateOp));
            }
        }
        stats.doneCreateOrUpdate(watch.elapsed(TimeUnit.NANOSECONDS),
                collection, Lists.transform(updateOps, new Function<UpdateOp, String>() {
                    @Override
                    public String apply(UpdateOp input) {
                        return input.getId();
                    }
                }));
        return new ArrayList<T>(results.values());
    }

    private <T extends Document> Map<String, T> readDocumentCached(Collection<T> collection, Set<String> keys) {
        Map<String, T> documents = new HashMap<String, T>();

        if (collection == Collection.NODES) {
            for (String key : keys) {
                NodeDocument cached = nodesCache.getIfPresent(key);
                if (cached != null && cached != NodeDocument.NULL) {
                    T doc = castAsT(unwrap(cached));
                    documents.put(doc.getId(), doc);
                }
            }
        }

        Set<String> documentsToRead = Sets.difference(keys, documents.keySet());
        Map<String, T> readDocuments = readDocumentsUncached(collection, documentsToRead);
        documents.putAll(readDocuments);

        if (collection == Collection.NODES) {
            for (T doc : readDocuments.values()) {
                nodesCache.putIfAbsent((NodeDocument) doc);
            }
        }

        return documents;
    }

    private <T extends Document> Map<String, T> readDocumentsUncached(Collection<T> collection, Set<String> keys) {
        Map<String, T> result = new HashMap<String, T>();

        Connection connection = null;
        RDBTableMetaData tmd = getTable(collection);
        try {
            connection = this.ch.getROConnection();
            List<RDBRow> rows = db.read(connection, tmd, keys);

            int size = rows.size();
            for (int i = 0; i < size; i++) {
                RDBRow row = rows.set(i, null);
                T document = convertFromDBObject(collection, row);
                result.put(document.getId(), document);
            }
            connection.commit();
        } catch (Exception ex) {
            throw asDocumentStoreException(ex, "trying to read: " + keys);
        } finally {
            this.ch.closeConnection(connection);
        }
        return result;
    }

    @CheckForNull
    private <T extends Document> CacheChangesTracker obtainTracker(Collection<T> collection, Set<String> keys) {
        if (collection == Collection.NODES) {
            return this.nodesCache.registerTracker(keys);
        } else {
            return null;
        }
    }

    @CheckForNull
    private <T extends Document> CacheChangesTracker obtainTracker(Collection<T> collection, String fromKey, String toKey) {
        if (collection == Collection.NODES) {
            return this.nodesCache.registerTracker(fromKey, toKey);
        } else {
            return null;
        }
    }

    private <T extends Document> Map<UpdateOp, T> bulkUpdate(Collection<T> collection, List<UpdateOp> updates, Map<String, T> oldDocs, boolean upsert) {
        Set<String> missingDocs = new HashSet<String>();
        for (UpdateOp op : updates) {
            if (!oldDocs.containsKey(op.getId())) {
                missingDocs.add(op.getId());
            }
        }
        oldDocs.putAll(readDocumentsUncached(collection, missingDocs));

        try (CacheChangesTracker tracker = obtainTracker(collection, Sets.union(oldDocs.keySet(), missingDocs) )) {
            List<T> docsToUpdate = new ArrayList<T>(updates.size());
            Set<String> keysToUpdate = new HashSet<String>();
            for (UpdateOp update : updates) {
                String id = update.getId();
                T modifiedDoc = collection.newDocument(this);
                if (oldDocs.containsKey(id)) {
                    oldDocs.get(id).deepCopy(modifiedDoc);
                }
                UpdateUtils.applyChanges(modifiedDoc, update);
                docsToUpdate.add(modifiedDoc);
                keysToUpdate.add(id);
            }

            Connection connection = null;
            RDBTableMetaData tmd = getTable(collection);
            try {
                connection = this.ch.getRWConnection();
                Set<String> successfulUpdates = db.update(connection, tmd, docsToUpdate, upsert);
                connection.commit();

                Set<String> failedUpdates = Sets.difference(keysToUpdate, successfulUpdates);
                oldDocs.keySet().removeAll(failedUpdates);

                if (collection == Collection.NODES) {
                    List<NodeDocument> docsToCache = new ArrayList<>();
                    for (T doc : docsToUpdate) {
                        if (successfulUpdates.contains(doc.getId())) {
                            docsToCache.add((NodeDocument) doc);
                        }
                    }
                    nodesCache.putNonConflictingDocs(tracker, docsToCache);
                }

                Map<UpdateOp, T> result = new HashMap<UpdateOp, T>();
                for (UpdateOp op : updates) {
                    if (successfulUpdates.contains(op.getId())) {
                        result.put(op, oldDocs.get(op.getId()));
                    }
                }
                return result;
            } catch (SQLException ex) {
                this.ch.rollbackConnection(connection);
                throw handleException("update failed for: " + keysToUpdate, ex, collection, keysToUpdate);
            } finally {
                this.ch.closeConnection(connection);
            }
        }
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        return internalCreateOrUpdate(collection, update, false, true);
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        for (CacheValue key : nodesCache.keys()) {
            invalidateCache(Collection.NODES, key.toString());
        }
        return null;
    }

    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        for (String key : keys) {
            invalidateCache(Collection.NODES, key);
        }
        return null;
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String id) {
        invalidateCache(collection, id, false);
    }

    private <T extends Document> void invalidateCache(Collection<T> collection, String id, boolean remove) {
        if (collection == Collection.NODES) {
            invalidateNodesCache(id, remove);
        }
    }

    private void invalidateNodesCache(String id, boolean remove) {
        try (CacheLock lock = acquireLockFor(id)) {
            if (remove) {
                nodesCache.invalidate(id);
            } else {
                nodesCache.markChanged(id);
                NodeDocument entry = nodesCache.getIfPresent(id);
                if (entry != null) {
                    entry.markUpToDate(0);
                }
            }
        }
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        Connection connection = null;
        try {
            connection = this.ch.getROConnection();
            long result = this.db.determineServerTimeDifferenceMillis(connection);
            connection.commit();
            return result;
        } catch (SQLException ex) {
            LOG.error("Trying to determine time difference to server", ex);
            throw asDocumentStoreException(ex, "Trying to determine time difference to server");
        } finally {
            this.ch.closeConnection(connection);
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
    static class RDBTableMetaData {

        private final String catalog;
        private final String name;
        private boolean idIsBinary = false;
        private boolean hasVersion = false;
        private boolean hasSplitDocs = false;
        private int dataLimitInOctets = 16384;
        private String schemaInfo = "";
        private String indexInfo = "";
        private Set<String> columnOnlyProperties = Collections.unmodifiableSet(COLUMNPROPERTIES);
        private Set<String> columnProperties = Collections.unmodifiableSet(COLUMNPROPERTIES);

        public RDBTableMetaData(@CheckForNull String catalog, @Nonnull String name) {
            this.catalog = catalog == null ? "" : catalog;
            this.name = name;
        }

        public int getDataLimitInOctets() {
            return this.dataLimitInOctets;
        }

        public String getCatalog() {
            return this.catalog;
        }

        public Set<String> getColumnProperties() {
            return this.columnProperties;
        }

        public Set<String> getColumnOnlyProperties() {
            return this.columnOnlyProperties;
        }

        public String getIndexInfo() {
            return this.indexInfo;
        }

        public String getName() {
            return this.name;
        }

        public String getSchemaInfo() {
            return this.schemaInfo;
        }

        public boolean isIdBinary() {
            return this.idIsBinary;
        }

        public boolean hasSplitDocs() {
            return this.hasSplitDocs;
        }

        public boolean hasVersion() {
            return this.hasVersion;
        }

        public void setIdIsBinary(boolean idIsBinary) {
            this.idIsBinary = idIsBinary;
        }

        public void setHasSplitDocs(boolean hasSplitDocs) {
            this.hasSplitDocs = hasSplitDocs;
            this.columnProperties = Collections.unmodifiableSet(hasSplitDocs ? COLUMNPROPERTIES2 : COLUMNPROPERTIES) ;
        }

        public void setHasVersion(boolean hasVersion) {
            this.hasVersion = hasVersion;
        }

        public void setDataLimitInOctets(int dataLimitInOctets) {
            this.dataLimitInOctets = dataLimitInOctets;
        }

        public void setSchemaInfo(String schemaInfo) {
            this.schemaInfo = schemaInfo;
        }

        public void setIndexInfo(String indexInfo) {
            this.indexInfo = indexInfo;
        }
    }

    private final Map<Collection<? extends Document>, RDBTableMetaData> tableMeta = new HashMap<Collection<? extends Document>, RDBTableMetaData>();

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
        try {
            this.nodesCache.close();
        } catch (IOException ex) {
            LOG.warn("Error occurred while closing nodes cache", ex);
        }
        LOG.info("RDBDocumentStore (" + getModuleVersion() + ") disposed" + getCnStats()
                + (this.droppedTables.isEmpty() ? "" : " (tables dropped: " + this.droppedTables + ")"));
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String id) {
        if (collection != Collection.NODES) {
            return null;
        } else {
            NodeDocument doc = nodesCache.getIfPresent(id);
            doc = (doc != null) ? unwrap(doc) : null;
            return castAsT(doc);
        }
    }

    private <T extends Document> T getIfCached(Collection<T> collection, String id, long modCount) {
        T doc = getIfCached(collection, id);
        if (doc != null && doc.getModCount() == modCount) {
            return doc;
        } else {
            return null;
        }
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return nodesCache.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Statistics are generated for each table. The following fields are always
     * added:
     * <dl>
     * <dt><em>tableName</em>.ns</dt>
     * <dd>fully qualified name of the database table</dd>
     * <dt><em>tableName</em>.schemaInfo</dt>
     * <dd>DDL information for table, as obtained during startup</dd>
     * <dt><em>tableName</em>.indexInfo</dt>
     * <dd>DDL information for associated indexes, as obtained during
     * startup</dd>
     * <dt><em>tableName</em>.count</dt>
     * <dd>exact number of rows</dd>
     * </dl>
     * In addition, some statistics information for
     * {@link Collection#CLUSTER_NODES} is added:
     * <dl>
     * <dt>clusterNodes.updates</dt>
     * <dd>Writes to the table, counted by cluster node ID</dd>
     * </dl>
     * Finally, additional database-specific statistics may be added; see
     * descriptions in
     * {@link RDBDocumentStoreDB#getAdditionalStatistics(RDBConnectionHandler, String, String)}
     * for details.
     **/
    @Nonnull
    @Override
    public Map<String, String> getStats() {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        tableMeta.forEach((k, v) -> toMapBuilder(builder, k, v));
        if (LOG.isDebugEnabled()) {
            LOG.debug("statistics obtained: " + builder.toString());
        }
        return builder.build();
    }

    private <T extends Document> void toMapBuilder(ImmutableMap.Builder<String, String> builder, Collection<T> collection, RDBTableMetaData meta) {
        String prefix = collection.toString();
        builder.put(prefix + ".ns", meta.getCatalog() + "." + meta.getName());
        builder.put(prefix + ".schemaInfo", meta.getSchemaInfo());
        builder.put(prefix + ".indexInfo", meta.getIndexInfo());
        if (Collection.CLUSTER_NODES.equals(collection)) {
            builder.put(prefix + ".updates", getCnStats());
        }
        // live data
        Map<String, String> map = this.dbInfo.getAdditionalStatistics(this.ch, meta.getCatalog(), meta.getName());
        map.forEach((k, v) -> builder.put(prefix + "." + k, v));
        try {
            long c = queryCount(collection, null, null, Collections.emptyList(), Collections.emptyList());
            builder.put(prefix + ".count", Long.toString(c));
        }
        catch (DocumentStoreException ex) {
            LOG.debug("getting entry count for " + prefix, ex);
        }
    }

    // implementation

    private static final String MODIFIED = "_modified";
    private static final String MODCOUNT = "_modCount";

    /**
     * Optional counter for changes to "_collisions" map ({@link NodeDocument#COLLISIONS}).
     */
    public static final String COLLISIONSMODCOUNT = "_collisionsModCount";

    private static final String ID = "_id";

    private static final Logger LOG = LoggerFactory.getLogger(RDBDocumentStore.class);

    private Exception callStack;

    private RDBConnectionHandler ch;

    // from options
    private Set<String> tablesToBeDropped = new HashSet<String>();

    // ratio between Java characters and UTF-8 encoding
    // a) single characters will fit into 3 bytes
    // b) a surrogate pair (two Java characters) will fit into 4 bytes
    // thus...
    public static final int CHAR2OCTETRATIO = 3;

    // number of retries for updates
    private static final int RETRIES = 10;

    // see OAK-2044
    protected static final boolean USECMODCOUNT = true;

    // Database schema supported by this version
    protected static final int SCHEMA = 2;

    private static final Key MODIFIEDKEY = new Key(MODIFIED, null);

    // DB-specific information
    private RDBDocumentStoreDB dbInfo;

    // utility class for performing low-level operations
    private RDBDocumentStoreJDBC db;

    protected static final List<String> EMPTY_KEY_PATTERN = Collections.emptyList();

    private Map<String, String> metadata;

    private DocumentStoreStatsCollector stats;

    // VERSION column mapping in queries used by RDBVersionGCSupport
    public static String VERSIONPROP = "__version";
    
    // set of supported indexed properties
    private static final Set<String> INDEXEDPROPERTIES = new HashSet<String>(Arrays.asList(new String[] { MODIFIED,
            NodeDocument.HAS_BINARY_FLAG, NodeDocument.DELETED_ONCE, NodeDocument.SD_TYPE, NodeDocument.SD_MAX_REV_TIME_IN_SECS, VERSIONPROP }));

    // set of required table columns
    private static final Set<String> REQUIREDCOLUMNS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
            new String[] { "id", "dsize", "deletedonce", "bdata", "data", "cmodcount", "modcount", "hasbinary", "modified" })));

    // set of optional table columns
    private static final Set<String> OPTIONALCOLUMNS = Collections
            .unmodifiableSet(new HashSet<String>(Arrays.asList(new String[] { "version", "sdtype", "sdmaxrevtime" })));

    // set of properties not serialized to JSON
    private static final Set<String> COLUMNPROPERTIES = new HashSet<String>(Arrays.asList(
            new String[] { ID, NodeDocument.HAS_BINARY_FLAG, NodeDocument.DELETED_ONCE, COLLISIONSMODCOUNT, MODIFIED, MODCOUNT }));
    // set of properties not serialized to JSON, schema version 2
    private static final Set<String> COLUMNPROPERTIES2 = new HashSet<String>(Arrays.asList(
            new String[] { ID, NodeDocument.HAS_BINARY_FLAG, NodeDocument.DELETED_ONCE, COLLISIONSMODCOUNT, MODIFIED, MODCOUNT,
                    NodeDocument.SD_TYPE, NodeDocument.SD_MAX_REV_TIME_IN_SECS, VERSIONPROP }));

    private final RDBDocumentSerializer ser = new RDBDocumentSerializer(this);

    private void initialize(DataSource ds, DocumentNodeStoreBuilder<?> builder, RDBOptions options) throws Exception {
        this.stats = builder.getDocumentStoreStatsCollector();

        this.callStack = LOG.isDebugEnabled() ? new Exception("call stack of RDBDocumentStore creation") : null;

        this.ch = new RDBConnectionHandler(ds);
        Connection con = this.ch.getRWConnection();
        String catalog = con.getCatalog();
        DatabaseMetaData md = con.getMetaData();
        if (null == catalog) {
            // Oracle
            catalog = md.getUserName();
        }

        this.tableMeta.put(Collection.NODES,
                new RDBTableMetaData(catalog, createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.NODES))));
        this.tableMeta.put(Collection.CLUSTER_NODES,
                new RDBTableMetaData(catalog, createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.CLUSTER_NODES))));
        this.tableMeta.put(Collection.JOURNAL,
                new RDBTableMetaData(catalog, createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.JOURNAL))));
        this.tableMeta.put(Collection.SETTINGS,
                new RDBTableMetaData(catalog, createTableName(options.getTablePrefix(), TABLEMAP.get(Collection.SETTINGS))));


        this.locks = new StripedNodeDocumentLocks();
        this.nodesCache = builder.buildNodeDocumentCache(this, locks);

        int isolation = con.getTransactionIsolation();
        String isolationDiags = RDBJDBCTools.isolationLevelToString(isolation);
        if (isolation != Connection.TRANSACTION_READ_COMMITTED) {
            LOG.info("Detected transaction isolation level " + isolationDiags + " is "
                    + (isolation < Connection.TRANSACTION_READ_COMMITTED ? "lower" : "higher") + " than expected "
                    + RDBJDBCTools.isolationLevelToString(Connection.TRANSACTION_READ_COMMITTED)
                    + " - check datasource configuration");
        }

        String dbDesc = String.format("%s %s (%d.%d)", md.getDatabaseProductName(), md.getDatabaseProductVersion(),
                md.getDatabaseMajorVersion(), md.getDatabaseMinorVersion()).replaceAll("[\r\n\t]", " ").trim();
        String driverDesc = String.format("%s %s (%d.%d)", md.getDriverName(), md.getDriverVersion(), md.getDriverMajorVersion(),
                md.getDriverMinorVersion()).replaceAll("[\r\n\t]", " ").trim();
        String dbUrl = md.getURL();

        this.dbInfo = RDBDocumentStoreDB.getValue(md.getDatabaseProductName());
        this.db = new RDBDocumentStoreJDBC(this.dbInfo, this.ser, QUERYHITSLIMIT, QUERYTIMELIMIT);
        this.metadata = ImmutableMap.<String,String>builder()
                .put("type", "rdb")
                .put("db", md.getDatabaseProductName())
                .put("version", md.getDatabaseProductVersion())
                .put("driver", md.getDriverName())
                .put("driverVersion", md.getDriverVersion())
                .build();
        String versionDiags = dbInfo.checkVersion(md);
        if (!versionDiags.isEmpty()) {
            LOG.error(versionDiags);
        }

        if (! "".equals(dbInfo.getInitializationStatement())) {
            Statement stmt = null;
            try {
                stmt = con.createStatement();
                stmt.execute(dbInfo.getInitializationStatement());
                stmt.close();
                con.commit();
            }
            finally {
                closeStatement(stmt);
            }
        }

        List<String> tablesCreated = new ArrayList<String>();
        List<String> tablesPresent = new ArrayList<String>();
        try {
            createTableFor(con, Collection.CLUSTER_NODES, this.tableMeta.get(Collection.CLUSTER_NODES), tablesCreated,
                    tablesPresent, options.getInitialSchema(), options.getUpgradeToSchema());
            createTableFor(con, Collection.NODES, this.tableMeta.get(Collection.NODES), tablesCreated, tablesPresent,
                    options.getInitialSchema(), options.getUpgradeToSchema());
            createTableFor(con, Collection.SETTINGS, this.tableMeta.get(Collection.SETTINGS), tablesCreated, tablesPresent,
                    options.getInitialSchema(), options.getUpgradeToSchema());
            createTableFor(con, Collection.JOURNAL, this.tableMeta.get(Collection.JOURNAL), tablesCreated, tablesPresent,
                    options.getInitialSchema(), options.getUpgradeToSchema());
        } finally {
            con.commit();
            con.close();
        }

        StringBuilder tableDiags = new StringBuilder();
        RDBTableMetaData nodesMeta = this.tableMeta.get(Collection.NODES);
        tableDiags.append(nodesMeta.getSchemaInfo());
        if (!nodesMeta.getIndexInfo().isEmpty()) {
            tableDiags.append(" /* ").append(nodesMeta.getIndexInfo()).append(" */");
        }

        if (options.isDropTablesOnClose()) {
            tablesToBeDropped.addAll(tablesCreated);
        }

        if (tableDiags.length() != 0) {
            tableDiags.insert(0, ", ");
        }

        String diag = dbInfo.getAdditionalDiagnostics(this.ch, this.tableMeta.get(Collection.NODES).getName());

        LOG.info("RDBDocumentStore (" + getModuleVersion() + ") instantiated for database " + dbDesc + ", using driver: "
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

    private static void obtainFlagsFromResultSetMeta(ResultSetMetaData met, RDBTableMetaData tmd) throws SQLException {

        for (int i = 1; i <= met.getColumnCount(); i++) {
            String lcName = met.getColumnName(i).toLowerCase(Locale.ENGLISH);
            if ("id".equals(lcName)) {
                tmd.setIdIsBinary(isBinaryType(met.getColumnType(i)));
            }
            if ("data".equals(lcName)) {
                tmd.setDataLimitInOctets(met.getPrecision(i));
            }
            if ("version".equals(lcName)) {
                tmd.setHasVersion(true);
            }
            if ("sdtype".equals(lcName)) {
                tmd.setHasSplitDocs(true);
            }
        }
    }

    private static String asQualifiedDbName(String one, String two) {
        one = Strings.nullToEmpty(one).trim();
        two = Strings.nullToEmpty(two).trim();

        if (one.isEmpty() && two.isEmpty()) {
            return null;
        } else {
            one = Strings.nullToEmpty(one).trim();
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

    private static String dumpIndexData(DatabaseMetaData met, ResultSetMetaData rmet, String tableName, Set<String> indexedColumns) {

        ResultSet rs = null;
        try {
            // if the result set metadata provides a table name, use that (the
            // other one
            // might be inaccurate due to case insensitivity issues
            String rmetTableName = Strings.nullToEmpty(rmet.getTableName(1)).trim();
            if (!rmetTableName.isEmpty()) {
                tableName = rmetTableName;
            }

            String rmetSchemaName = Strings.nullToEmpty(rmet.getSchemaName(1)).trim();

            rs = met.getIndexInfo(null, null, tableName, false, true);

            Map<String, Map<String, Object>> indices = getIndexInformation(rs, rmetSchemaName);
            if (indices.isEmpty() && !tableName.equals(tableName.toUpperCase(Locale.ENGLISH))) {
                // might have failed due to the DB's handling on ucase/lcase,
                // retry ucase
                rs = met.getIndexInfo(null, null, tableName.toUpperCase(Locale.ENGLISH), false, true);
                indices = getIndexInformation(rs, rmetSchemaName);
            }
            if (indexedColumns != null) {
                for (Map<String, Object> map : indices.values()) {
                    if (map.containsKey("columns")) {
                        indexedColumns.addAll((Set<String>) (map.get("columns")));
                    }
                }
            }
            return dumpIndexData(indices);
        } catch (SQLException ex) {
            // well it was best-effort
            return String.format("/* exception while retrieving index information: %s, code %d, state %s */", ex.getMessage(),
                    ex.getErrorCode(), ex.getSQLState());
        } finally {
            closeResultSet(rs);
        }
    }

    private static String dumpIndexData(Map<String, Map<String, Object>> indices) {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, Map<String, Object>> index : indices.entrySet()) {
            String indexName = index.getKey();
            Map<String, Object> info = index.getValue();
            boolean nonUnique = ((Boolean) index.getValue().get("nonunique"));
            Map<Integer, String> fields = (Map<Integer, String>) info.get("fields");
            if (!fields.isEmpty()) {
                if (sb.length() != 0) {
                    sb.append(", ");
                }
                sb.append(String.format("%sindex %s on %s (", nonUnique ? "" : "unique ", indexName, info.get("tname")));
                String delim = "";
                for (String field : fields.values()) {
                    sb.append(delim);
                    delim = ", ";
                    sb.append(field);
                }
                sb.append(")");
                sb.append(" ").append(info.get("type"));
            }
            Object filterCondition = info.get("filterCondition");
            if (filterCondition != null) {
                sb.append(" where ").append(filterCondition.toString());
            }
            sb.append(String.format(" (#%s, p%s)", info.get("cardinality").toString(), info.get("pages").toString()));
        }
        return sb.toString();
    }

    // see https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getIndexInfo(java.lang.String,%20java.lang.String,%20java.lang.String,%20boolean,%20boolean)
    private static Map<String, Map<String, Object>> getIndexInformation(ResultSet rs, String rmetSchemaName) throws SQLException {
        Map<String, Map<String, Object>> result = new TreeMap<String, Map<String, Object>>();
        while (rs.next()) {
            String name = asQualifiedDbName(rs.getString("INDEX_QUALIFIER"), rs.getString("INDEX_NAME"));
            if (name != null) {
                Map<String, Object> info = result.get(name);
                if (info == null) {
                    info = new HashMap<String, Object>();
                    result.put(name, info);
                    info.put("fields", new TreeMap<Integer, String>());
                }
                info.put("nonunique", rs.getBoolean("NON_UNIQUE"));
                info.put("type", indexTypeAsString(rs.getInt("TYPE")));
                String inSchema = rs.getString("TABLE_SCHEM");
                inSchema = Strings.nullToEmpty(inSchema).trim();
                String filterCondition = Strings.nullToEmpty(rs.getString("FILTER_CONDITION")).trim();
                if (!filterCondition.isEmpty()) {
                    info.put("filterCondition", filterCondition);
                }
                info.put("cardinality", rs.getInt("CARDINALITY"));
                info.put("pages", rs.getInt("PAGES"));
                Set<String> columns = new HashSet<String>();
                info.put("columns", columns);
                // skip indices on tables in other schemas in case we have that information
                if (rmetSchemaName.isEmpty() || inSchema.isEmpty() || rmetSchemaName.equals(inSchema)) {
                    String tname = asQualifiedDbName(inSchema, rs.getString("TABLE_NAME"));
                    info.put("tname", tname);
                    String cname = rs.getString("COLUMN_NAME");
                    if (cname != null) {
                        columns.add(cname.toUpperCase(Locale.ENGLISH));
                        String order = "A".equals(rs.getString("ASC_OR_DESC")) ? " ASC" : ("D".equals(rs.getString("ASC_OR_DESC")) ? " DESC" : "");
                        ((Map<Integer, String>) info.get("fields")).put(rs.getInt("ORDINAL_POSITION"), cname + order);
                    }
                }
            }
        }
        return result;
    }

    private void createTableFor(Connection con, Collection<? extends Document> col, RDBTableMetaData tmd, List<String> tablesCreated,
            List<String> tablesPresent, int initialSchema, int upgradeToSchema) throws SQLException {

        String dbname = this.dbInfo.toString();
        if (con.getMetaData().getURL() != null) {
            dbname += " (" + con.getMetaData().getURL() + ")";
        }
        String tableName = tmd.getName();

        Statement checkStatement = null;

        ResultSet checkResultSet = null;
        Statement creatStatement = null;

        try {
            // avoid PreparedStatement due to weird DB2 behavior (OAK-6237)
            checkStatement = con.createStatement();
            checkResultSet = checkStatement.executeQuery("select * from " + tableName + " where ID = '0'");

            // try to discover size of DATA column and binary-ness of ID
            ResultSetMetaData met = checkResultSet.getMetaData();
            obtainFlagsFromResultSetMeta(met, tmd);

            // check that all required columns are present
            Set<String> requiredColumns = new HashSet<String>(REQUIREDCOLUMNS);
            Set<String> unknownColumns = new HashSet<String>();
            boolean hasVersionColumn = false;
            boolean hasSDTypeColumn = false;
            for (int i = 1; i <= met.getColumnCount(); i++) {
                String cname = met.getColumnName(i).toLowerCase(Locale.ENGLISH);
                if (!requiredColumns.remove(cname)) {
                    if (!OPTIONALCOLUMNS.contains(cname)) {
                        unknownColumns.add(cname);
                    }
                }
                if (cname.equals("version")) {
                    hasVersionColumn = true;
                }
                if (cname.equals("sdtype")) {
                    hasSDTypeColumn = true;
                }
            }

            if (!requiredColumns.isEmpty()) {
                String message = String.format("Table %s: the following required columns are missing: %s", tableName,
                        requiredColumns.toString());
                LOG.error(message);
                throw new DocumentStoreException(message);
            }

            if (!unknownColumns.isEmpty()) {
                String message = String.format("Table %s: the following columns are unknown and will not be maintained: %s",
                        tableName, unknownColumns.toString());
                LOG.info(message);
            }

            String tableInfo = RDBJDBCTools.dumpResultSetMeta(met);
            tmd.setSchemaInfo(tableInfo);
            Set<String> indexOn = new HashSet<String>();
            String indexInfo = dumpIndexData(con.getMetaData(), met, tableName, indexOn);
            tmd.setIndexInfo(indexInfo);

            closeResultSet(checkResultSet);
            boolean dbWasChanged = false;

            if (!hasVersionColumn && upgradeToSchema >= 1) {
                dbWasChanged |= upgradeTable(con, tableName, 1);
            }

            if (!hasSDTypeColumn && upgradeToSchema >= 2) {
                dbWasChanged |= upgradeTable(con, tableName, 2);
            }

            if (!indexOn.contains("MODIFIED") && col == Collection.NODES) {
                dbWasChanged |= addModifiedIndex(con, tableName);
            }

            tablesPresent.add(tableName);

            if (dbWasChanged) {
                getTableMetaData(con, col, tmd);
            }
        } catch (SQLException ex) {
            // table does not appear to exist
            con.rollback();

            try {
                creatStatement = con.createStatement();
                creatStatement.execute(this.dbInfo.getTableCreationStatement(tableName, initialSchema));
                creatStatement.close();

                for (String ic : this.dbInfo.getIndexCreationStatements(tableName, initialSchema)) {
                    creatStatement = con.createStatement();
                    creatStatement.execute(ic);
                    creatStatement.close();
                }

                con.commit();

                if (initialSchema < 1 && upgradeToSchema >= 1) {
                    upgradeTable(con, tableName, 1);
                }

                if (initialSchema < 2 && upgradeToSchema >= 2) {
                    upgradeTable(con, tableName, 2);
                }

                tablesCreated.add(tableName);

                getTableMetaData(con, col, tmd);
            }
            catch (SQLException ex2) {
                LOG.error("Failed to create table " + tableName + " in " + dbname, ex2);
                throw ex2;
            }
        }
        finally {
            closeResultSet(checkResultSet);
            closeStatement(checkStatement);
            closeStatement(creatStatement);
        }
    }

    private boolean upgradeTable(Connection con, String tableName, int level) throws SQLException {
        boolean wasChanged = false;
        
        for (String statement : this.dbInfo.getTableUpgradeStatements(tableName, level)) {
            Statement upgradeStatement = null;
            try {
                upgradeStatement = con.createStatement();
                upgradeStatement.execute(statement);
                upgradeStatement.close();
                con.commit();
                LOG.info("Upgraded " + tableName + " to DB level " + level + " using '" + statement + "'");
                wasChanged = true;
            } catch (SQLException exup) {
                con.rollback();
                LOG.info("Attempted to upgrade " + tableName + " to DB level " + level + " using '" + statement
                        + "', but failed - will continue without.", exup);
            } finally {
                closeStatement(upgradeStatement);
            }
        }

        return wasChanged;
    }

    private boolean addModifiedIndex(Connection con, String tableName) throws SQLException {
        boolean wasChanged = false;

        String statement = this.dbInfo.getModifiedIndexStatement(tableName);
        Statement upgradeStatement = null;
        try {
            upgradeStatement = con.createStatement();
            upgradeStatement.execute(statement);
            upgradeStatement.close();
            con.commit();
            LOG.info("Added modified index to " + tableName + " using '" + statement + "'");
            wasChanged = true;
        } catch (SQLException exup) {
            con.rollback();
            LOG.info("Attempted to add modified index to " + tableName + " using '" + statement
                    + "', but failed - will continue without.", exup);
        } finally {
            closeStatement(upgradeStatement);
        }

        return wasChanged;
    }
    
    private static void getTableMetaData(Connection con, Collection<? extends Document> col, RDBTableMetaData tmd) throws SQLException {
        Statement checkStatement = null;
        ResultSet checkResultSet = null;

        try {
            checkStatement = con.createStatement();
            checkResultSet = checkStatement.executeQuery("select * from " + tmd.getName() + " where ID = '0'");

            // try to discover size of DATA column and binary-ness of ID
            ResultSetMetaData met = checkResultSet.getMetaData();
            obtainFlagsFromResultSetMeta(met, tmd);

            String tableInfo = RDBJDBCTools.dumpResultSetMeta(met);
            tmd.setSchemaInfo(tableInfo);
            String indexInfo = dumpIndexData(con.getMetaData(), met, tmd.getName(), null);
            tmd.setIndexInfo(indexInfo);
        } finally {
            closeResultSet(checkResultSet);
            closeStatement(checkStatement);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (!this.ch.isClosed() && this.callStack != null) {
            LOG.debug("finalizing RDBDocumentStore that was not disposed", this.callStack);
        }
        super.finalize();
    }

    private <T extends Document> T readDocumentCached(final Collection<T> collection, final String id, int maxCacheAge) {
        if (collection != Collection.NODES) {
            return readDocumentUncached(collection, id, null);
        } else {
            NodeDocument doc = null;
            if (maxCacheAge > 0) {
                // first try without lock
                doc = nodesCache.getIfPresent(id);
                if (doc != null) {
                    long lastCheckTime = doc.getLastCheckTime();
                    if (lastCheckTime != 0) {
                        if (maxCacheAge == Integer.MAX_VALUE || System.currentTimeMillis() - lastCheckTime < maxCacheAge) {
                            stats.doneFindCached(Collection.NODES, id);
                            return castAsT(unwrap(doc));
                        }
                    }
                }
            }
            try {
                try (CacheLock lock = acquireLockFor(id)) {
                    // caller really wants the cache to be cleared
                    if (maxCacheAge == 0) {
                        invalidateNodesCache(id, true);
                        doc = null;
                    }
                    final NodeDocument cachedDoc = doc;
                    doc = nodesCache.get(id, new Callable<NodeDocument>() {
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
                        nodesCache.put(doc);
                    }
                }
                return castAsT(unwrap(doc));
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to load document with " + id, e);
            }
        }
    }

    @CheckForNull
    private <T extends Document> boolean internalCreate(Collection<T> collection, List<UpdateOp> updates) {
        final Stopwatch watch = startWatch();
        List<String> ids = new ArrayList<String>(updates.size());
        boolean success = true;
        try {

            // try up to CHUNKSIZE ops in one transaction
            for (List<UpdateOp> chunks : Lists.partition(updates, CHUNKSIZE)) {
                List<T> docs = new ArrayList<T>();
                for (UpdateOp update : chunks) {
                    ids.add(update.getId());
                    maintainUpdateStats(collection, update.getId());
                    UpdateUtils.assertUnconditional(update);
                    T doc = collection.newDocument(this);
                    addUpdateCounters(update);
                    UpdateUtils.applyChanges(doc, update);
                    if (!update.getId().equals(doc.getId())) {
                        throw new DocumentStoreException("ID mismatch - UpdateOp: " + update.getId() + ", ID property: "
                                + doc.getId());
                    }
                    docs.add(doc);
                }
                boolean done = insertDocuments(collection, docs);
                if (done) {
                    if (collection == Collection.NODES) {
                        for (T doc : docs) {
                            nodesCache.putIfAbsent((NodeDocument) doc);
                        }
                    }
                } else {
                    success = false;
                }
            }
            return success;
        } catch (DocumentStoreException ex) {
            return false;
        } finally {
            stats.doneCreate(watch.elapsed(TimeUnit.NANOSECONDS), collection, ids, success);
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
            addUpdateCounters(update);
            UpdateUtils.applyChanges(doc, update);
            try {
                Stopwatch watch = startWatch();
                if (!insertDocuments(collection, Collections.singletonList(doc))) {
                    throw new DocumentStoreException("Can't insert the document: " + doc.getId());
                }
                if (collection == Collection.NODES) {
                    nodesCache.putIfAbsent((NodeDocument) doc);
                }
                stats.doneFindAndModify(watch.elapsed(TimeUnit.NANOSECONDS), collection, update.getId(), true, true, 0);
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
                throw new DocumentStoreException("update of " + update.getId() + " failed, race condition?", null,
                        DocumentStoreException.Type.TRANSIENT);
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
        if (checkConditions && !UpdateUtils.checkConditions(oldDoc, update.getConditions())) {
            return null;
        } else {
            maintainUpdateStats(collection, update.getId());
            addUpdateCounters(update);
            T doc = createNewDocument(collection, oldDoc, update);
            final Stopwatch watch = startWatch();
            boolean success = false;
            int retries = maxRetries;
            try (CacheLock lock = acquireLockFor(update.getId())) {
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

                        if (checkConditions && !UpdateUtils.checkConditions(oldDoc, update.getConditions())) {
                            return null;
                        }
                        else {
                            addUpdateCounters(update);
                            doc = createNewDocument(collection, oldDoc, update);
                        }
                    } else {
                        if (collection == Collection.NODES) {
                            nodesCache.replaceCachedDocument((NodeDocument) oldDoc, (NodeDocument) doc);
                        }
                    }
                }

                if (!success) {
                    throw new DocumentStoreException("failed update of " + doc.getId() + " (race?) after " + maxRetries
                            + " retries", null, DocumentStoreException.Type.TRANSIENT);
                }

                return oldDoc;
            } finally {
                int numOfAttempts = maxRetries - retries - 1;
                stats.doneFindAndModify(watch.elapsed(TimeUnit.NANOSECONDS), collection,
                        update.getId(), false, success, numOfAttempts);
            }
        }
    }

    @Nonnull
    private <T extends Document> T createNewDocument(Collection<T> collection, T oldDoc, UpdateOp update) {
        T doc = collection.newDocument(this);
        oldDoc.deepCopy(doc);
        UpdateUtils.applyChanges(doc, update);
        doc.seal();
        return doc;
    }

    private static void addUpdateCounters(UpdateOp update) {
        if (hasChangesToCollisions(update)) {
            update.increment(COLLISIONSMODCOUNT, 1);
        }
        update.increment(MODCOUNT, 1);
    }

    private <T extends Document> List<T> internalQuery(Collection<T> collection, String fromKey, String toKey,
            List<String> excludeKeyPatterns, List<QueryCondition> conditions, int limit) {
        Connection connection = null;
        RDBTableMetaData tmd = getTable(collection);
        for (QueryCondition cond : conditions) {
            if (!INDEXEDPROPERTIES.contains(cond.getPropertyName())) {
                String message = "indexed property " + cond.getPropertyName() + " not supported, query was '" + cond
                        + "'; supported properties are " + INDEXEDPROPERTIES;
                LOG.info(message);
                throw new DocumentStoreException(message);
            }
        }

        final Stopwatch watch = startWatch();
        int resultSize = 0;

        try (CacheChangesTracker tracker = obtainTracker(collection, fromKey, toKey)) {
            long now = System.currentTimeMillis();
            connection = this.ch.getROConnection();
            String from = collection == Collection.NODES && NodeDocument.MIN_ID_VALUE.equals(fromKey) ? null : fromKey;
            String to = collection == Collection.NODES && NodeDocument.MAX_ID_VALUE.equals(toKey) ? null : toKey;

            // OAK-6839: only populate the cache with *new* entries if the query
            // isn't open-ended (something done by GC processes)
            boolean populateCache = to != null;

            List<RDBRow> dbresult = db.query(connection, tmd, from, to, excludeKeyPatterns, conditions, limit);
            connection.commit();

            int size = dbresult.size();
            List<T> result = new ArrayList<T>(size);
            for (int i = 0; i < size; i++) {
                // free RDBRow as early as possible
                RDBRow row = dbresult.set(i, null);
                T doc = getIfCached(collection, row.getId(), row.getModcount());
                if (doc == null) {
                    // parse DB contents into document if and only if it's not
                    // already in the cache
                    doc = convertFromDBObject(collection, row);
                } else {
                    // we got a document from the cache, thus collection is NODES
                    // and a tracker is present
                    long lastmodified = modifiedOf(doc);
                    if (lastmodified == row.getModified() && lastmodified >= 1) {
                        try (CacheLock lock = acquireLockFor(row.getId())) {
                            if (!tracker.mightBeenAffected(row.getId())) {
                                // otherwise mark it as fresh
                                ((NodeDocument) doc).markUpToDate(now);
                            }
                        }
                    }
                    else {
                        // we need a fresh document instance
                        doc = convertFromDBObject(collection, row);
                    }
                }
                result.add(doc);
            }
            if (collection == Collection.NODES) {
                if (populateCache) {
                    nodesCache.putNonConflictingDocs(tracker, castAsNodeDocumentList(result));
                } else {
                    Map<String, ModificationStamp> invMap = Maps.newHashMap();
                    for (Document doc : result) {
                        invMap.put(doc.getId(), new ModificationStamp(modcountOf(doc), modifiedOf(doc)));
                    }
                    nodesCache.invalidateOutdated(invMap);
                }
            }
            resultSize = result.size();
            return result;
        } catch (Exception ex) {
            LOG.error("SQL exception on query", ex);
            throw asDocumentStoreException(ex, "SQL exception on query");
        } finally {
            this.ch.closeConnection(connection);
            stats.doneQuery(watch.elapsed(TimeUnit.NANOSECONDS), collection, fromKey, toKey,
                    !conditions.isEmpty(), resultSize, -1, false);
        }
    }

    private static interface MyCloseableIterable<T> extends Closeable, Iterable<T> {
    }

    protected <T extends Document> Iterable<T> queryAsIterable(final Collection<T> collection, String fromKey, String toKey,
            final List<String> excludeKeyPatterns, final List<QueryCondition> conditions, final int limit, final String sortBy) {

        final RDBTableMetaData tmd = getTable(collection);
        Set<String> allowedProps = Sets.intersection(INDEXEDPROPERTIES, tmd.getColumnProperties());
        for (QueryCondition cond : conditions) {
            if (!allowedProps.contains(cond.getPropertyName())) {
                String message = "indexed property " + cond.getPropertyName() + " not supported, query was '" + cond
                        + "'; supported properties are " + allowedProps;
                LOG.info(message);
                throw new UnsupportedIndexedPropertyException(message);
            }
        }

        final String from = collection == Collection.NODES && NodeDocument.MIN_ID_VALUE.equals(fromKey) ? null : fromKey;
        final String to = collection == Collection.NODES && NodeDocument.MAX_ID_VALUE.equals(toKey) ? null : toKey;

        return new MyCloseableIterable<T>() {

            Set<Iterator<RDBRow>> returned = Sets.newHashSet();

            @Override
            public Iterator<T> iterator() {
                try {
                    Iterator<RDBRow> res = db.queryAsIterator(ch, tmd, from, to, excludeKeyPatterns, conditions,
                            limit, sortBy);
                    returned.add(res);
                    Iterator<T> tmp = Iterators.transform(res, new Function<RDBRow, T>() {
                        @Override
                        public T apply(RDBRow input) {
                            return convertFromDBObject(collection, input);
                        }
                    });
                    return CloseableIterator.wrap(tmp, (Closeable) res);
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public void close() throws IOException {
                for (Iterator<RDBRow> rdbi : returned) {
                    if (rdbi instanceof Closeable) {
                        ((Closeable) rdbi).close();
                    }
                }
            }
        };
    }

    protected <T extends Document> long queryCount(final Collection<T> collection, String fromKey, String toKey,
            final List<String> excludeKeyPatterns, final List<QueryCondition> conditions) {

        return internalGetAggregate(collection, "COUNT", "*", fromKey, toKey, excludeKeyPatterns, conditions);
    }

    protected <T extends Document> long getMinValue(final Collection<T> collection, String field, String fromKey, String toKey,
            final List<String> excludeKeyPatterns, final List<QueryCondition> conditions) {

        return internalGetAggregate(collection, "MIN", field, fromKey, toKey, excludeKeyPatterns, conditions);
    }

    private <T extends Document> long internalGetAggregate(final Collection<T> collection, final String aggregrate, String field,
            String fromKey, String toKey, final List<String> excludeKeyPatterns, final List<QueryCondition> conditions) {

        final RDBTableMetaData tmd = getTable(collection);
        for (QueryCondition cond : conditions) {
            if (!INDEXEDPROPERTIES.contains(cond.getPropertyName())) {
                String message = "indexed property " + cond.getPropertyName() + " not supported, query was '" + cond
                        + "'; supported properties are " + INDEXEDPROPERTIES;
                LOG.info(message);
                throw new DocumentStoreException(message);
            }
        }

        final String from = collection == Collection.NODES && NodeDocument.MIN_ID_VALUE.equals(fromKey) ? null : fromKey;
        final String to = collection == Collection.NODES && NodeDocument.MAX_ID_VALUE.equals(toKey) ? null : toKey;

        Connection connection = null;
        try {
            connection = ch.getROConnection();
            long result = db.getLong(connection, tmd, aggregrate, field, from, to, excludeKeyPatterns, conditions);
            connection.commit();
            return result;
        } catch (SQLException ex) {
            LOG.error("SQL exception on query", ex);
            throw asDocumentStoreException(ex, "SQL exception on query");
        } finally {
            this.ch.closeConnection(connection);
        }
    }

    @Nonnull
    protected <T extends Document> RDBTableMetaData getTable(Collection<T> collection) {
        RDBTableMetaData tmd = this.tableMeta.get(collection);
        if (tmd != null) {
            return tmd;
        } else {
            throw new IllegalArgumentException("Unknown collection: " + collection.toString());
        }
    }

    @CheckForNull
    private <T extends Document> T readDocumentUncached(Collection<T> collection, String id, NodeDocument cachedDoc) {
        Connection connection = null;
        RDBTableMetaData tmd = getTable(collection);
        final Stopwatch watch = startWatch();
        boolean docFound = true;
        try {
            long lastmodcount = -1, lastmodified = -1;
            if (cachedDoc != null) {
                lastmodcount = modcountOf(cachedDoc);
                lastmodified = modifiedOf(cachedDoc);
            }
            connection = this.ch.getROConnection();
            RDBRow row = db.read(connection, tmd, id, lastmodcount, lastmodified);
            connection.commit();
            if (row == null) {
                docFound = false;
                return null;
            } else {
                if (lastmodcount == row.getModcount() && lastmodified == row.getModified() && lastmodified >= 1) {
                    // we can re-use the cached document
                    cachedDoc.markUpToDate(System.currentTimeMillis());
                    return castAsT(cachedDoc);
                } else {
                    return convertFromDBObject(collection, row);
                }
            }
        } catch (Exception ex) {
            throw asDocumentStoreException(ex, "exception while reading " + id);
        } finally {
            this.ch.closeConnection(connection);
            stats.doneFindUncached(watch.elapsed(TimeUnit.NANOSECONDS), collection, id, docFound, false);
        }
    }

    private <T extends Document> void delete(Collection<T> collection, String id) {
        Connection connection = null;
        RDBTableMetaData tmd = getTable(collection);
        Stopwatch watch = startWatch();
        try {
            connection = this.ch.getRWConnection();
            db.delete(connection, tmd, Collections.singletonList(id));
            connection.commit();
        } catch (Exception ex) {
            this.ch.rollbackConnection(connection);
            throw handleException("removing " + id, ex, collection, id);
        } finally {
            this.ch.closeConnection(connection);
            stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, 1);
        }
    }

    private <T extends Document> int delete(Collection<T> collection, List<String> ids) {
        int numDeleted = 0;
        RDBTableMetaData tmd = getTable(collection);
        for (List<String> sublist : Lists.partition(ids, 64)) {
            Connection connection = null;
            Stopwatch watch = startWatch();
            try {
                connection = this.ch.getRWConnection();
                numDeleted += db.delete(connection, tmd, sublist);
                connection.commit();
            } catch (Exception ex) {
                this.ch.rollbackConnection(connection);
                throw handleException("removing " + ids, ex, collection, ids);
            } finally {
                this.ch.closeConnection(connection);
                stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, ids.size());
            }
        }
        return numDeleted;
    }

    private <T extends Document> int delete(Collection<T> collection, Map<String, Long> toRemove) {
        int numDeleted = 0;
        RDBTableMetaData tmd = getTable(collection);
        Map<String, Long> subMap = Maps.newHashMap();
        Iterator<Entry<String, Long>> it = toRemove.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Long> entry = it.next();
            subMap.put(entry.getKey(), entry.getValue());
            if (subMap.size() == 64 || !it.hasNext()) {
                Connection connection = null;
                int num = 0;
                Stopwatch watch = startWatch();
                try {
                    connection = this.ch.getRWConnection();
                    num = db.delete(connection, tmd, subMap);
                    numDeleted += num;
                    connection.commit();
                } catch (Exception ex) {
                    this.ch.rollbackConnection(connection);
                    Set<String> ids = subMap.keySet();
                    throw handleException("deleting " + ids, ex, collection, ids);
                } finally {
                    this.ch.closeConnection(connection);
                    stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, num);
                }
                subMap.clear();
            }
        }
        return numDeleted;
    }

    private <T extends Document> int deleteWithCondition(Collection<T> collection, List<QueryCondition> conditions) {
        int numDeleted = 0;
        RDBTableMetaData tmd = getTable(collection);
        Stopwatch watch = startWatch();
        Connection connection = null;
        try {
            connection = this.ch.getRWConnection();
            numDeleted = db.deleteWithCondition(connection, tmd, conditions);
            connection.commit();
        } catch (Exception ex) {
            this.ch.rollbackConnection(connection);
            throw asDocumentStoreException(ex, "deleting " + collection + ": " + conditions);
        } finally {
            this.ch.closeConnection(connection);
            stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, numDeleted);
        }
        return numDeleted;
    }

    private <T extends Document> boolean updateDocument(@Nonnull Collection<T> collection, @Nonnull T document,
            @Nonnull UpdateOp update, Long oldmodcount) {
        Connection connection = null;
        RDBTableMetaData tmd = getTable(collection);
        String data = null;
        try {
            connection = this.ch.getRWConnection();
            Number hasBinary = (Number) document.get(NodeDocument.HAS_BINARY_FLAG);
            Boolean deletedOnce = (Boolean) document.get(NodeDocument.DELETED_ONCE);
            Long modcount = (Long) document.get(MODCOUNT);
            Long cmodcount = (Long) document.get(COLLISIONSMODCOUNT);
            boolean success = false;
            boolean shouldRetry = true;

            // every 16th update is a full rewrite
            if (isAppendableUpdate(update) && modcount % 16 != 0) {
                String appendData = ser.asString(update, tmd.getColumnOnlyProperties());
                if (appendData.length() < tmd.getDataLimitInOctets() / CHAR2OCTETRATIO) {
                    try {
                        Operation modOperation = update.getChanges().get(MODIFIEDKEY);
                        long modified = getModifiedFromOperation(modOperation);
                        boolean modifiedIsConditional = modOperation == null || modOperation.type != UpdateOp.Operation.Type.SET;
                        success = db.appendingUpdate(connection, tmd, document.getId(), modified, modifiedIsConditional, hasBinary,
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
                data = ser.asString(document, tmd.getColumnOnlyProperties());
                Object m = document.get(MODIFIED);
                long modified = (m instanceof Long) ? ((Long)m).longValue() : 0;
                success = db.update(connection, tmd, document.getId(), modified, hasBinary, deletedOnce, modcount, cmodcount,
                        oldmodcount, data);
                connection.commit();
            }
            return success;
        } catch (SQLException ex) {
            this.ch.rollbackConnection(connection);
            String addDiags = "";
            if (data != null && RDBJDBCTools.matchesSQLState(ex, "22", "72")) {
                byte[] bytes = asBytes(data);
                addDiags = String.format(" (DATA size in Java characters: %d, in octets: %d, computed character limit: %d)",
                        data.length(), bytes.length, tmd.getDataLimitInOctets() / CHAR2OCTETRATIO);
            }
            String message = String.format("Update for %s failed%s", document.getId(), addDiags);
            LOG.debug(message, ex);
            throw handleException(message, ex, collection, document.getId());
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

    private static boolean isAppendableUpdate(UpdateOp update) {
        return NOAPPEND == false;
    }

    private static long getModifiedFromOperation(Operation op) {
        return op == null ? 0L : Long.parseLong(op.value.toString());
    }

    private <T extends Document> boolean insertDocuments(Collection<T> collection, List<T> documents) {
        Connection connection = null;
        RDBTableMetaData tmd = getTable(collection);
        try {
            connection = this.ch.getRWConnection();
            Set<String> insertedKeys = db.insert(connection, tmd, documents);
            connection.commit();
            return insertedKeys.size() == documents.size();
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
                    String data = ser.asString(d, tmd.getColumnOnlyProperties());
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

            throw handleException(message, ex, collection, ids);
        } finally {
            this.ch.closeConnection(connection);
        }
    }

    // configuration

    // Whether to use GZIP compression
    private static final boolean NOGZIP = Boolean
            .getBoolean("org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.NOGZIP");
    // Whether to use append operations (string concatenation) in the DATA column
    private static final boolean NOAPPEND = Boolean
            .getBoolean("org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.NOAPPEND");
    // Number of documents to insert at once for batch create
    private static final int CHUNKSIZE = Integer.getInteger(
            "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.CHUNKSIZE", 64);
    // Number of query hits above which a diagnostic warning is generated
    private static final int QUERYHITSLIMIT = Integer.getInteger(
            "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QUERYHITSLIMIT", 4096);
    // Number of elapsed ms in a query above which a diagnostic warning is generated
    private static final int QUERYTIMELIMIT = Integer.getInteger(
            "org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QUERYTIMELIMIT", 10000);
    // Whether to use JDBC batch commands for the createOrUpdate (default: true).
    private static final boolean BATCHUPDATES = Boolean.parseBoolean(System
            .getProperty("org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.BATCHUPDATES", "true"));

    public static byte[] asBytes(@Nonnull String data) {
        byte[] bytes;
        try {
            bytes = data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            LOG.error("UTF-8 not supported??", ex);
            throw asDocumentStoreException(ex, "UTF-8 not supported??");
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
                throw asDocumentStoreException(ex, "Error while gzipping contents");
            }
        }
    }


    @Override
    public void setReadWriteMode(String readWriteMode) {
        // ignored
    }

    public void setStatsCollector(DocumentStoreStatsCollector stats) {
        this.stats = stats;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Document> T castAsT(NodeDocument doc) {
        return (T) doc;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Document> List<NodeDocument> castAsNodeDocumentList(List<T> list) {
        return (List<NodeDocument>) list;
    }

    private NodeDocumentCache nodesCache;

    private NodeDocumentLocks locks;

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
        Long n = doc.getModCount();
        return n != null ? n : -1;
    }

    private static long modifiedOf(@Nonnull Document doc) {
        Object l = doc.get(NodeDocument.MODIFIED_IN_SECS);
        return (l instanceof Long) ? ((Long)l).longValue() : -1;
    }

    @Nonnull
    protected <T extends Document> T convertFromDBObject(@Nonnull Collection<T> collection, @Nonnull RDBRow row) {
        // this method is present here in order to facilitate unit testing for OAK-3566
        return ser.fromRow(collection, row);
    }

    private static boolean hasChangesToCollisions(UpdateOp update) {
        if (!USECMODCOUNT) {
            return false;
        } else {
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
    }

    // keeping track of CLUSTER_NODES updates
    private Map<String, Long> cnUpdates = new ConcurrentHashMap<String, Long>();

    private <T extends Document> void maintainUpdateStats(Collection<T> collection, String key) {
        if (collection == Collection.CLUSTER_NODES) {
            synchronized (this) {
                Long old = cnUpdates.get(key);
                old = old == null ? Long.valueOf(1) : old + 1;
                cnUpdates.put(key, old);
            }
        }
    }

    private String getCnStats() {
        if (cnUpdates.isEmpty()) {
            return "";
        } else {
            List<Map.Entry<String, Long>> tmp = new ArrayList<Map.Entry<String, Long>>();
            tmp.addAll(cnUpdates.entrySet());
            Collections.sort(tmp, new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }});
            return " (Cluster Node updates: " + tmp.toString() + ")";
        }
    }

    private Stopwatch startWatch() {
        return Stopwatch.createStarted();
    }

    protected NodeDocumentCache getNodeDocumentCache() {
        return nodesCache;
    }

    private <T extends Document> DocumentStoreException handleException(String message, Exception ex, Collection<T> collection,
            java.util.Collection<String> ids) {
        if (collection == Collection.NODES) {
            for (String id : ids) {
                invalidateCache(collection, id, false);
            }
        }
        return asDocumentStoreException(ex, message);
    }

    private <T extends Document> DocumentStoreException handleException(String message, Exception ex, Collection<T> collection,
            String id) {
        return handleException(message, ex, collection, Collections.singleton(id));
    }

    protected class UnsupportedIndexedPropertyException extends DocumentStoreException {

        private static final long serialVersionUID = -8392572622365260105L;

        public UnsupportedIndexedPropertyException(String message) {
            super(message);
        }
    }

    private CacheLock acquireLockFor(String id) {
        return new CacheLock(this.locks, id);
    }

    private static class CacheLock implements AutoCloseable {

        private final Lock lock;

        public CacheLock(NodeDocumentLocks locks, String id) {
            this.lock = locks.acquire(id);
        }

        @Override
        public void close() {
            lock.unlock();
        }
    }

    // slightly extended query support
    protected static class QueryCondition {

        private final String propertyName, operator;
        private final List<? extends Object> operands;

        public QueryCondition(String propertyName, String operator, long value) {
            this.propertyName = propertyName;
            this.operator = operator;
            this.operands = Collections.singletonList(value);
        }

        public QueryCondition(String propertyName, String operator, List<? extends Object> values) {
            this.propertyName = propertyName;
            this.operator = operator;
            this.operands = values;
        }

        public QueryCondition(String propertyName, String operator) {
            this.propertyName = propertyName;
            this.operator = operator;
            this.operands = Collections.emptyList();
        }

        public String getPropertyName() {
            return propertyName;
        }

        public String getOperator() {
            return operator;
        }

        public List<? extends Object> getOperands() {
            return this.operands;
        }

        @Override
        public String toString() {
            if (this.operands.isEmpty()) {
                return String.format("%s %s", propertyName, operator);
            } else if (this.operands.size() == 1) {
                return String.format("%s %s %s", propertyName, operator, operands.get(0).toString());
            } else {
                return String.format("%s %s %s", propertyName, operator, operands.toString());
            }
        }
    }
}
