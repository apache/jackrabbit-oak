/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.stats.TimeSeriesStatsUtil.asCompositeData;

public class CompositeNodeStoreStats implements CompositeNodeStoreStatsMBean, CompositeNodeStoreMonitor {

    public static final String STRING_CACHE_SIZE = "STRING_CACHE_SIZE";

    public static final String NODE_PATH_DEPTH = "_PATH_DEPTH";

    public static final String NODE_SWITCH_TO_DEFAULT_MOUNT = "_SWITCH_TO_DEFAULT_MOUNT";

    public static final String NODE_SWITCH_TO_NON_DEFAULT_MOUNT = "_SWITCH_TO_NON_DEFAULT_MOUNT";

    private final StatisticsProvider statisticsProvider;

    private final CounterStats stringCacheSize;

    private final HistogramStats nodePathDepths;

    private final CounterStats nodeSwitchToDefaultMount;

    private final CounterStats nodeSwitchToNonDefaultMount;

    private final Map<String, Long> nodePathCounts;

    private long maxNodePathCount;

    private final long nodePathCountSizeLimit;

    private final long nodePathCountValueLimit;

    private final boolean countPaths;

    private final String prefix;

    public CompositeNodeStoreStats(StatisticsProvider statisticsProvider, String prefix, boolean countPaths) {
        this(statisticsProvider, prefix, countPaths, 100, Long.MAX_VALUE / 2);
    }

    public CompositeNodeStoreStats(StatisticsProvider statisticsProvider, String prefix, boolean countPaths, long nodePathCountSizeLimit, long nodePathCountValueLimit) {
        this.statisticsProvider = statisticsProvider;

        this.stringCacheSize = statisticsProvider.getCounterStats(prefix + STRING_CACHE_SIZE, StatsOptions.DEFAULT);
        this.nodePathDepths = statisticsProvider.getHistogram(prefix + NODE_PATH_DEPTH, StatsOptions.METRICS_ONLY);

        this.nodeSwitchToDefaultMount = statisticsProvider.getCounterStats(prefix + NODE_SWITCH_TO_DEFAULT_MOUNT, StatsOptions.DEFAULT);
        this.nodeSwitchToNonDefaultMount = statisticsProvider.getCounterStats(prefix + NODE_SWITCH_TO_NON_DEFAULT_MOUNT, StatsOptions.DEFAULT);

        this.nodePathCounts = newHashMap();
        this.maxNodePathCount = 0;

        this.countPaths = countPaths;
        this.nodePathCountSizeLimit = nodePathCountSizeLimit;
        this.nodePathCountValueLimit = nodePathCountValueLimit;

        this.prefix = prefix;
    }

    @Override
    public void onCreateNodeObject(String path) {
        nodePathDepths.update(PathUtils.getDepth(path));
        if (countPaths) {
            updatePathCount(path);
        }
    }

    @Override
    public void onSwitchNodeToNative(Mount mount) {
        if (mount.isDefault()) {
            nodeSwitchToDefaultMount.inc();
        } else {
            nodeSwitchToNonDefaultMount.inc();
        }
    }

    @Override
    public void onAddStringCacheEntry() {
        stringCacheSize.inc();
    }

    @Override
    public CompositeData getStringCacheSize() {
        return getCompositeData(STRING_CACHE_SIZE);
    }

    @Override
    public CompositeData getNodeSwitchToDefaultMount() {
        return getCompositeData(NODE_SWITCH_TO_DEFAULT_MOUNT);
    }

    @Override
    public CompositeData getNodeSwitchToNonDefaultMount() {
        return getCompositeData(NODE_SWITCH_TO_NON_DEFAULT_MOUNT);
    }

    private CompositeData getCompositeData(String name) {
        return asCompositeData(getTimeSeries(prefix + name), prefix + name);
    }

    @Override
    public TabularData getNodePathCounts() throws OpenDataException {
        return pathsTable(nodePathCounts, "popularNodeStatePaths", "Popular node state paths");
    }

    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }

    private synchronized void updatePathCount(String path) {
        long newValue = nodePathCounts.compute(path, (p, v) -> v == null ? 1 : v + 1);
        boolean removeZeros = false;
        if (newValue == 1) {
            if (nodePathCounts.size() >= nodePathCountSizeLimit) {
                nodePathCounts.entrySet().stream().forEach(e -> nodePathCounts.put(e.getKey(), e.getValue() - 1));
                maxNodePathCount--;
                removeZeros = true;
            }
        }
        if (maxNodePathCount < newValue) {
            maxNodePathCount = newValue;
            if (maxNodePathCount >= nodePathCountValueLimit) {
                nodePathCounts.entrySet().stream().forEach(e -> nodePathCounts.put(e.getKey(), e.getValue() / 2));
                maxNodePathCount /= 2;
                removeZeros = true;
            }
        }

        if (removeZeros) {
            Iterator<Long> it = nodePathCounts.values().iterator();
            while (it.hasNext()) {
                if (it.next() <= 0) {
                    it.remove();
                }
            }
        }
    }

    private TabularData pathsTable(Map<String, Long> paths, String name, String description) throws OpenDataException {
        CompositeType pathRowType = new CompositeType("compositePath", "Path",
                new String[]{"count", "path"},
                new String[]{"count", "path"},
                new OpenType[]{SimpleType.LONG, SimpleType.STRING});


        TabularDataSupport tabularData = new TabularDataSupport(
                new TabularType(name,
                        description,
                        pathRowType,
                        new String[]{"path"}
                ));

        paths.entrySet()
                .stream()
                .sorted(Comparator.<Entry<String, Long>>comparingLong(Entry::getValue).reversed())
                .map(e -> {
                    Map<String, Object> m = newHashMap();
                    m.put("count", e.getValue());
                    m.put("path", e.getKey());
                    return m;
                })
                .map(d -> mapToCompositeData(pathRowType, d))
                .forEach(tabularData::put);

        return tabularData;
    }

    private static CompositeData mapToCompositeData(CompositeType compositeType, Map<String, Object> data) {
        try {
            return new CompositeDataSupport(compositeType, data);
        } catch (OpenDataException e) {
            throw new IllegalArgumentException(e);
        }
    }

    Map<String, Long> getNodePathCountsMap() {
        return nodePathCounts;
    }

    long getMaxNodePathCount() {
        return maxNodePathCount;
    }
}
