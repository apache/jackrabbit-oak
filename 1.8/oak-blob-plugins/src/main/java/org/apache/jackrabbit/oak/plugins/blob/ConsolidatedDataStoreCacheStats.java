/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.blob;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import com.google.common.base.Strings;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.ConsolidatedDataStoreCacheStatsMBean;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.InMemoryDataRecord;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

/**
 * Stats for caching data store.
 */
@Component
public class ConsolidatedDataStoreCacheStats implements ConsolidatedDataStoreCacheStatsMBean {

    private final List<Registration> registrations = newArrayList();

    private final List<DataStoreCacheStatsMBean> cacheStats = newArrayList();

    @Reference public AbstractSharedCachingDataStore cachingDataStore;

    @Reference public NodeStore nodeStore;

    @Override
    public TabularData getCacheStats() {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(CacheStatsData.class.getName(),
                    "Consolidated DataStore Cache Stats", CacheStatsData.TYPE, new String[]{"name"});
            tds = new TabularDataSupport(tt);
            for(DataStoreCacheStatsMBean stats : cacheStats){
                tds.put(new CacheStatsData(stats).toCompositeData());
            }
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    @Activate
    private void activate(BundleContext context){
        Whiteboard wb = new OsgiWhiteboard(context);
        List<DataStoreCacheStatsMBean> allStats = cachingDataStore.getStats();
        for (DataStoreCacheStatsMBean stat : allStats) {
            registrations.add(
                registerMBean(wb, CacheStatsMBean.class, stat, CacheStatsMBean.TYPE,
                    stat.getName()));
            cacheStats.add(stat);
        }
        registrations.add(
            registerMBean(wb, ConsolidatedDataStoreCacheStatsMBean.class, this,
                ConsolidatedDataStoreCacheStatsMBean.TYPE,
                "Consolidated DataStore Cache statistics"));
    }

    @Deactivate
    private void deactivate(){
        for (Registration r : registrations) {
            r.unregister();
        }
        registrations.clear();
    }

    /**
     * Determines whether a file-like entity with the given name
     * has been "synced" (completely copied) to S3.
     *
     * Determination of "synced":
     * - A nodeName of null or "" is always "not synced".
     * - A nodeName that does not map to a valid node is always "not synced".
     * - If the node for this nodeName does not have a binary property,
     * this node is always "not synced" since such a node would never be
     * copied to S3.
     * - If the node for this nodeName is not in the nodeStore, this node is
     * always "not synced".
     * - Otherwise, the state is "synced" if the corresponding blob is
     * completely stored in S3.
     *
     * @param nodePathName - Path to the entity to check.  This is
     *                       a node path, not an external file path.
     * @return true if the file is synced to S3.
     */
    @Override
    public boolean isFileSynced(final String nodePathName) {
        if (Strings.isNullOrEmpty(nodePathName)) {
            return false;
        }

        if (null == nodeStore) {
            return false;
        }

        final NodeState leafNode = findLeafNode(nodePathName);
        if (!leafNode.exists()) {
            return false;
        }

        boolean nodeHasBinaryProperties = false;
        for (final PropertyState propertyState : leafNode.getProperties()) {
            nodeHasBinaryProperties |= (propertyState.getType() == Type.BINARY || propertyState.getType() == Type.BINARIES);
            try {
                if (propertyState.getType() == Type.BINARY) {
                    final Blob blob = (Blob) propertyState.getValue(propertyState.getType());
                    if (null == blob || !haveRecordForBlob(blob)) {
                        return false;
                    }
                } else if (propertyState.getType() == Type.BINARIES) {
                    final List<Blob> blobs = (List<Blob>) propertyState.getValue(propertyState.getType());
                    if (null == blobs) {
                        return false;
                    }
                    for (final Blob blob : blobs) {
                        if (!haveRecordForBlob(blob)) {
                            return false;
                        }
                    }
                }
            }
            catch (ClassCastException e) {
                return false;
            }
        }

        // If we got here and nodeHasBinaryProperties is true,
        // it means at least one binary property was found for
        // the leaf node and that we were able to locate a
        // records for binaries found.
        return nodeHasBinaryProperties;
    }

    private NodeState findLeafNode(final String nodePathName) {
        final Iterable<String> pathNodes = PathUtils.elements(PathUtils.getParentPath(nodePathName));
        final String leafNodeName = PathUtils.getName(nodePathName);

        NodeState currentNode = nodeStore.getRoot();
        for (String pathNodeName : pathNodes) {
            if (pathNodeName.length() > 0) {
                NodeState childNode = currentNode.getChildNode(pathNodeName);
                if (!childNode.exists()) {
                    break;
                }
                currentNode = childNode;
            }
        }
        return currentNode.getChildNode(leafNodeName);
    }

    private boolean haveRecordForBlob(final Blob blob) {
        final String fullBlobId = blob.getContentIdentity();
        if (!Strings.isNullOrEmpty(fullBlobId)
            && !InMemoryDataRecord.isInstance(fullBlobId)) {
            String blobId = DataStoreBlobStore.BlobId.of(fullBlobId).getBlobId();
            return cachingDataStore.exists(new DataIdentifier(blobId));
        }
        return false;
    }

    private static class CacheStatsData {
        static final String[] FIELD_NAMES = new String[]{
                "name",
                "requestCount",
                "hitCount",
                "hitRate",
                "missCount",
                "missRate",
                "loadCount",
                "loadSuccessCount",
                "loadExceptionCount",
                "totalLoadTime",
                "averageLoadPenalty",
                "evictionCount",
                "elementCount",
                "totalWeight",
                "totalMemWeight",
                "maxWeight",
        };

        static final String[] FIELD_DESCRIPTIONS = FIELD_NAMES;

        @SuppressWarnings("rawtypes")
        static final OpenType[] FIELD_TYPES = new OpenType[]{
                SimpleType.STRING,
                SimpleType.LONG,
                SimpleType.LONG,
                SimpleType.BIGDECIMAL,
                SimpleType.LONG,
                SimpleType.BIGDECIMAL,
                SimpleType.LONG,
                SimpleType.LONG,
                SimpleType.LONG,
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.LONG,
                SimpleType.LONG,
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.STRING,
        };

        static final CompositeType TYPE = createCompositeType();

        static CompositeType createCompositeType() {
            try {
                return new CompositeType(
                        CacheStatsData.class.getName(),
                        "Composite data type for Cache statistics",
                        CacheStatsData.FIELD_NAMES,
                        CacheStatsData.FIELD_DESCRIPTIONS,
                        CacheStatsData.FIELD_TYPES);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }

        private final DataStoreCacheStatsMBean stats;

        public CacheStatsData(DataStoreCacheStatsMBean stats){
            this.stats = stats;
        }

        CompositeDataSupport toCompositeData() {
            Object[] values = new Object[]{
                    stats.getName(),
                    stats.getRequestCount(),
                    stats.getHitCount(),
                    new BigDecimal(stats.getHitRate(),new MathContext(2)),
                    stats.getMissCount(),
                    new BigDecimal(stats.getMissRate(), new MathContext(2)),
                    stats.getLoadCount(),
                    stats.getLoadSuccessCount(),
                    stats.getLoadExceptionCount(),
                    timeInWords(stats.getTotalLoadTime()),
                    TimeUnit.NANOSECONDS.toMillis((long) stats.getAverageLoadPenalty()) + "ms",
                    stats.getEvictionCount(),
                    stats.getElementCount(),
                    humanReadableByteCount(stats.estimateCurrentWeight()),
                    humanReadableByteCount(stats.estimateCurrentMemoryWeight()),
                    humanReadableByteCount(stats.getMaxTotalWeight()),
            };
            try {
                return new CompositeDataSupport(TYPE, FIELD_NAMES, values);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static String timeInWords(long nanos) {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        return String.format("%d min, %d sec",
            TimeUnit.MILLISECONDS.toMinutes(millis),
            TimeUnit.MILLISECONDS.toSeconds(millis) -
                TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
    }
}
