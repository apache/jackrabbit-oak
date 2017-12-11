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

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

public class QueryStatsData {
    
    private final String query;
    private final String language;
    private final long createdMillis = System.currentTimeMillis();
    private boolean internal;
    private String lastThreadName;
    private long lastExecutedMillis;
    private long executeCount;
    
    /**
     * Rows read by iterating over the query result.
     */
    private long totalRowsRead;
    private long maxRowsRead;
    
    /**
     *  Rows returned by the index.
     */
    private long totalRowsScanned;
    private long maxRowsScanned;
    private long planNanos;
    private long readNanos;
    private long maxTimeNanos;
    private boolean captureStackTraces;

    public QueryStatsData(String query, String language) {
        this.query = query;
        this.language = language;
    }
    
    public String getKey() {
        return query + "/" + language;
    }
    
    /**
     * The maximum CPU time needed to run one query.
     * 
     * @return the time in nanoseconds
     */
    public long getMaxTimeNanos() {
        return maxTimeNanos;
    }
    
    public long getTotalTimeNanos() {
        return planNanos + readNanos;
    }
    
    public long getMaxRowsScanned() {
        return maxRowsScanned;
    }
    
    public void setCaptureStackTraces(boolean captureStackTraces) {
        this.captureStackTraces = captureStackTraces;
    }
    
    public long getCreatedMillis() {
        return createdMillis;
    }
    

    public long getExecuteCount() {
        return executeCount;
    }

    public long getTotalRowsRead() {
        return totalRowsRead;
    }

    public long getTotalRowsScanned() {
        return totalRowsScanned;
    }

    public String getLanguage() {
        return language;
    }

    public String getQuery() {
        return query;
    }

    public boolean isInternal() {
        return internal;
    }

    public String getLastThreadName() {
        return lastThreadName;
    }

    public long getLastExecutedMillis() {
        return lastExecutedMillis;
    }
    
    @Override
    public String toString() {
        return new JsopBuilder().object().
            key("createdMillis").value(getTimeString(createdMillis)).
            key("lastExecutedMillis").value(getTimeString(lastExecutedMillis)).
            key("executeCount").value(executeCount).
            key("totalRowsRead").value(totalRowsRead).
            key("maxRowsRead").value(maxRowsRead).
            key("totalRowsScanned").value(totalRowsScanned).
            key("maxRowsScanned").value(maxRowsScanned).
            key("planNanos").value(planNanos).
            key("readNanos").value(readNanos).
            key("maxTimeNanos").value(maxTimeNanos).
            key("internal").value(internal).
            key("query").value(query).
            key("language").value(language).
            key("lastThreadName").value(lastThreadName).
        endObject().toString();
    }
    
    public static final String getTimeString(long timeMillis) {
        return String.format("%tF %tT", timeMillis, timeMillis);
    }

    public class QueryExecutionStats {
        
        long time;
        
        public void execute(long nanos) {
            QueryRecorder.record(query, internal);
            executeCount++;
            lastExecutedMillis = System.currentTimeMillis();
            time += nanos;
            planNanos += nanos;
            maxTimeNanos = Math.max(maxTimeNanos, time);
        }

        public void setInternal(boolean b) {
            internal = b;
        }

        public void setThreadName(String name) {
            if (captureStackTraces) {
                StringBuilder buff = new StringBuilder();
                for(StackTraceElement e : Thread.currentThread().getStackTrace()) {
                    buff.append("\n\tat " + e);
                }
                name = name + buff.toString();
            }
            lastThreadName = name;
        }

        public void read(long count, long max, long nanos) {
            totalRowsRead += count;
            maxRowsRead = Math.max(maxRowsRead, max);
            time += nanos;
            readNanos += nanos;
            maxTimeNanos = Math.max(maxTimeNanos, time);
        }

        public void scan(long count, long max) {
            totalRowsScanned += count;
            maxRowsScanned = Math.max(maxRowsScanned, max);
        }        
    }

}