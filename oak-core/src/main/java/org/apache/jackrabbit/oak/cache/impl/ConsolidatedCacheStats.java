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

package org.apache.jackrabbit.oak.cache.impl;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.ConsolidatedCacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.PersistentCacheStatsMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;

import static org.apache.jackrabbit.oak.cache.CacheStats.timeInWords;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(service = {})
public class ConsolidatedCacheStats implements ConsolidatedCacheStatsMBean {

    private Tracker<CacheStatsMBean> cacheStats;
    private Tracker<PersistentCacheStatsMBean> persistentCacheStats;

    private Registration mbeanReg;

    @Override
    public TabularData getCacheStats() {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(CacheStatsData.class.getName(),
                    "Consolidated Cache Stats", CacheStatsData.TYPE, new String[]{"name"});
            tds = new TabularDataSupport(tt);
            for(CacheStatsMBean stats : cacheStats.getServices()){
                tds.put(new CacheStatsData(stats).toCompositeData());
            }
            for(CacheStatsMBean stats : persistentCacheStats.getServices()){
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
        cacheStats = wb.track(CacheStatsMBean.class);
        persistentCacheStats = wb.track(PersistentCacheStatsMBean.class);
        mbeanReg = registerMBean(wb,
                ConsolidatedCacheStatsMBean.class,
                this,
                ConsolidatedCacheStatsMBean.TYPE,
                "Consolidated Cache statistics");
    }

    @Deactivate
    private void deactivate(){
        if(mbeanReg != null){
            mbeanReg.unregister();
        }

        if(cacheStats != null){
            cacheStats.stop();
        }

        if(persistentCacheStats != null) {
            persistentCacheStats.stop();
        }
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

        private final CacheStatsMBean stats;

        public CacheStatsData(CacheStatsMBean stats){
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
                    humanReadableByteCount(stats.getMaxTotalWeight()),
            };
            try {
                return new CompositeDataSupport(TYPE, FIELD_NAMES, values);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

}
