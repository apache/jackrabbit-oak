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
package org.apache.jackrabbit.oak.query.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData.QueryExecutionStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStatsMBeanImpl extends AnnotatedStandardMBean 
        implements QueryStatsMBean, QueryStatsReporter {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final int SLOW_QUERY_LIMIT_SCANNED = 
            Integer.getInteger("oak.query.slowScanLimit", 100000);
    private final int MAX_STATS_DATA = 
            Integer.getInteger("oak.query.stats", 5000);
    private final int MAX_POPULAR_QUERIES = 
            Integer.getInteger("oak.query.slowLimit", 100);
    private final ConcurrentSkipListMap<String, QueryStatsData> statistics = 
            new ConcurrentSkipListMap<String, QueryStatsData>();
    private final QueryEngineSettings settings;
    private boolean captureStackTraces;
    private int evictionCount;

    public QueryStatsMBeanImpl(QueryEngineSettings settings) {
        super(QueryStatsMBean.class);
        this.settings = settings;
    }
    
    @Override
    public TabularData getSlowQueries() {
        ArrayList<QueryStatsData> list = new ArrayList<QueryStatsData>();
        long maxScanned = Math.min(SLOW_QUERY_LIMIT_SCANNED, settings.getLimitReads());
        for(QueryStatsData s : statistics.values()) {
            if(s.getMaxRowsScanned() > maxScanned) {
                list.add(s);
            }
        }
        Collections.sort(list, new Comparator<QueryStatsData>() {
            @Override
            public int compare(QueryStatsData o1, QueryStatsData o2) {
                return -Long.compare(o1.getMaxRowsScanned(), o2.getMaxRowsScanned());
            }
        });
        return asTabularData(list);
    }
    
    @Override
    public TabularData getPopularQueries() {
        ArrayList<QueryStatsData> list = new ArrayList<QueryStatsData>(statistics.values());
        Collections.sort(list, new Comparator<QueryStatsData>() {
            @Override
            public int compare(QueryStatsData o1, QueryStatsData o2) {
                return -Long.compare(o1.getTotalTimeNanos(), o2.getTotalTimeNanos());
            }
        });
        while (list.size() > MAX_POPULAR_QUERIES) {
            list.remove(list.size() - 1);
        }
        return asTabularData(list);
    }

    @Override
    public void resetStats() {
        statistics.clear();
    }
    
    @Override
    public void setCaptureStackTraces(boolean captureStackTraces) {
        this.captureStackTraces = captureStackTraces;
    }

    @Override
    public boolean getCaptureStackTraces() {
        return captureStackTraces;
    }
    
    @Override
    public String asJson() {
        ArrayList<QueryStatsData> list = new ArrayList<QueryStatsData>(statistics.values());
        Collections.sort(list, new Comparator<QueryStatsData>() {
            @Override
            public int compare(QueryStatsData o1, QueryStatsData o2) {
                return -Long.compare(o1.getTotalTimeNanos(), o2.getTotalTimeNanos());
            }
        });
        StringBuilder buff = new StringBuilder();
        buff.append("[\n");
        int i = 0;
        for(QueryStatsData s : list) {
            if (i++ > 0) {
                buff.append(",\n");
            }
            buff.append(s.toString());
        }
        return buff.append("\n]\n").toString();
    }

    @Override
    public QueryExecutionStats getQueryExecution(String statement, String language) {
        if (log.isTraceEnabled()) {
            log.trace("Execute " + language + " / " + statement);
        }
        if (statistics.size() > 2 * MAX_STATS_DATA) {
            evict();
        }
        QueryStatsData stats = new QueryStatsData(statement, language);
        QueryStatsData s2 = statistics.putIfAbsent(stats.getKey(), stats);
        if (s2 != null) {
            stats = s2;
        }
        stats.setCaptureStackTraces(captureStackTraces);
        return stats.new QueryExecutionStats();
    }

    private void evict() {
        evictionCount++;
        // retain 50% of the slowest entries
        // of the rest, retain the newest entries 
        ArrayList<QueryStatsData> list = new ArrayList<QueryStatsData>(statistics.values());
        Collections.sort(list, new Comparator<QueryStatsData>() {
            @Override
            public int compare(QueryStatsData o1, QueryStatsData o2) {
                int comp = -Long.compare(o1.getTotalTimeNanos(), o2.getTotalTimeNanos());
                if (comp == 0) {
                    comp = -Long.compare(o1.getCreatedMillis(), o2.getCreatedMillis());
                }
                return comp;
            }
        });
        Collections.sort(list.subList(MAX_STATS_DATA / 2, MAX_STATS_DATA), new Comparator<QueryStatsData>() {
            @Override
            public int compare(QueryStatsData o1, QueryStatsData o2) {
                return -Long.compare(o1.getCreatedMillis(), o2.getCreatedMillis());
            }
        });
        for (int i = MAX_STATS_DATA; i < list.size(); i++) {
            statistics.remove(list.get(i).getKey());
        }
    }
    
    public int getEvictionCount() {
        return evictionCount;
    }
    
    private TabularData asTabularData(ArrayList<QueryStatsData> list) {
        TabularDataSupport tds = null;
        try {
            CompositeType ct = QueryStatsCompositeTypeFactory.getCompositeType();
            TabularType tt = new TabularType(QueryStatsData.class.getName(),
                    "Query History", ct, QueryStatsCompositeTypeFactory.index);
            tds = new TabularDataSupport(tt);
            int position = 1;
            for (QueryStatsData q : list) {
                tds.put(new CompositeDataSupport(ct,
                        QueryStatsCompositeTypeFactory.names,
                        QueryStatsCompositeTypeFactory.getValues(q, position++)));
            }
            return tds;
        } catch (Exception e) {
            log.debug("Error", e);
            return null;
        }
    }
    
    private static class QueryStatsCompositeTypeFactory {

        private final static String[] index = { "position" };

        private final static String[] names = { "position", 
                "maxTimeMillis", "totalTimeMillis", "executeCount", 
                "rowsRead", "rowsScanned", "maxRowsScanned",
                "language", "statement", "lastExecuted",
                "lastThread"};

        private final static String[] descriptions = names;

        @SuppressWarnings("rawtypes")
        private final static OpenType[] types = { SimpleType.LONG,
                SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, 
                SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, 
                SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
                SimpleType.STRING};

        public static CompositeType getCompositeType() throws OpenDataException {
            return new CompositeType(QueryStatsMBean.class.getName(),
                    QueryStatsMBean.class.getName(), names, descriptions, types);
        }

        public static Object[] getValues(QueryStatsData q, int position) {
            return new Object[] { (long) position,
                    q.getMaxTimeNanos() / 1000000, q.getTotalTimeNanos() / 1000000, q.getExecuteCount(), 
                    q.getTotalRowsRead(), q.getTotalRowsScanned(), q.getMaxRowsScanned(),
                    q.getLanguage(), q.getQuery(), QueryStatsData.getTimeString(q.getLastExecutedMillis()),
                    q.isInternal() ? "(internal query)" : q.getLastThreadName()};
        }
    }
    
}
