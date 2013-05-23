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
package org.apache.jackrabbit.mongomk.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;

/**
 * A MicroKernel wrapper that can be used to log and also time MicroKernel
 * calls.
 */
public class TimingWrapper implements MicroKernel {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("mk.debug", "true"));
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final MicroKernel mk;
    private final int id = NEXT_ID.getAndIncrement();

    private long startTime;
    private final Map<String, Count> counts = new HashMap<String, Count>();
    private long lastLogTime;

    /**
     * A class that keeps track of timing data and call counts.
     */
    static class Count {
        public long count;
        public long max;
        public long total;

        void update(long time) {
            count++;
            if (time > max) {
                max = time;
            }
            total += time;
        }
    }

    public TimingWrapper(MicroKernel mk) {
        this.mk = mk;
        counts.put("commit", new Count());
        counts.put("getHeadRevision", new Count());
        counts.put("getJournal", new Count());
        counts.put("diff", new Count());
        counts.put("getLength", new Count());
        counts.put("getNodes", new Count());
        counts.put("getRevisionHistory", new Count());
        counts.put("nodeExists", new Count());
        counts.put("getChildNodeCount", new Count());
        counts.put("read", new Count());
        counts.put("waitForCommit", new Count());
        counts.put("write", new Count());
        counts.put("branch", new Count());
        counts.put("merge", new Count());
    }

    @Override
    public String commit(String path, String jsonDiff, String revisionId, String message) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.commit(path, jsonDiff, revisionId, message);
            updateAndLogTimes("commit", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String getHeadRevision() {
        try {
            long start = System.currentTimeMillis();
            String result = mk.getHeadRevision();
            updateAndLogTimes("getHeadRevision", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.getJournal(fromRevisionId, toRevisionId, path);
            updateAndLogTimes("getJournal", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path, int depth) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.diff(fromRevisionId, toRevisionId, path, depth);
            updateAndLogTimes("diff", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public long getLength(String blobId) {
        try {
            long start = System.currentTimeMillis();
            long result = mk.getLength(blobId);
            updateAndLogTimes("getLength", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes, String filter) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
            updateAndLogTimes("getNodes", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.getRevisionHistory(since, maxEntries, path);
            updateAndLogTimes("getRevisionHistory", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        try {
            long start = System.currentTimeMillis();
            boolean result = mk.nodeExists(path, revisionId);
            updateAndLogTimes("nodeExists", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        try {
            long start = System.currentTimeMillis();
            long result = mk.getChildNodeCount(path, revisionId);
            updateAndLogTimes("getChildNodeCount", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        try {
            long start = System.currentTimeMillis();
            int result = mk.read(blobId, pos, buff, off, length);
            updateAndLogTimes("read", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long maxWaitMillis) throws InterruptedException {
        try {
            long start = System.currentTimeMillis();
            String result = mk.waitForCommit(oldHeadRevisionId, maxWaitMillis);
            updateAndLogTimes("waitForCommit", start);
            return result;
        } catch (InterruptedException e) {
            logException(e);
            throw e;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String write(InputStream in) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.write(in);
            updateAndLogTimes("write", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String branch(String trunkRevisionId) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.branch(trunkRevisionId);
            updateAndLogTimes("branch", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String merge(String branchRevisionId, String message) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.merge(branchRevisionId, message);
            updateAndLogTimes("merge", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Nonnull
    @Override
    public String rebase(@Nonnull String branchRevisionId, String newBaseRevisionId) {
        try {
            long start = System.currentTimeMillis();
            String result = mk.rebase(branchRevisionId, newBaseRevisionId);
            updateAndLogTimes("rebase", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public static String quote(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof String) {
            return JsopBuilder.encode((String) o);
        }
        return o.toString();
    }

    private RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        log("// unexpected exception type: " + e.getClass().getName());
        return new MicroKernelException("Unexpected exception: " + e.toString(), e);
    }

    private void logException(Exception e) {
        log("// exception: " + e.toString());
    }
    
    private void log(String message) {
        if (DEBUG) {
            System.out.println(id + " " + message);
        }
    }

    private void updateAndLogTimes(String operation, long start) {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }
        counts.get(operation).update(System.currentTimeMillis() - start);
        long now = System.currentTimeMillis();
        if (now - lastLogTime >= 2000) {
            lastLogTime = now;
            for (Entry<String, Count> count : counts.entrySet()) {
                double c = count.getValue().count;
                double max = count.getValue().max;
                double total = count.getValue().total;
                double avg = total / c;
                if (c > 0) {
                    log(count.getKey() + " --> count:" + c + " avg: " + avg
                            + " max: " + max + " total: " + total);
                }
            }
            System.out.println("Time: " + ((now - startTime) / 1000L));
            System.out.println("------");
        }
    }
}
