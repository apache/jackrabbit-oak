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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;

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
    private long totalLogTime;

    /**
     * A class that keeps track of timing data and call counts.
     */
    static class Count {
        public long count;
        public long max;
        public long total;
        public long paramSize;
        public long resultSize;

        void update(long time, int paramSize, int resultSize) {
            count++;
            if (time > max) {
                max = time;
            }
            total += time;
            this.paramSize += paramSize;
            this.resultSize += resultSize;
        }
    }

    public TimingWrapper(MicroKernel mk) {
        this.mk = mk;
        lastLogTime = now();
    }

    @Override
    public String commit(String path, String jsonDiff, String revisionId, String message) {
        try {
            long start = now();
            String result = mk.commit(path, jsonDiff, revisionId, message);
            updateAndLogTimes("commit", start, 
                    size(path) + size(jsonDiff) + size(message), 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String getHeadRevision() {
        try {
            long start = now();
            String result = mk.getHeadRevision();
            updateAndLogTimes("getHeadRevision", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override @Nonnull
    public String checkpoint(long lifetime) {
        try {
            long start = now();
            String result = mk.checkpoint(lifetime);
            updateAndLogTimes("checkpoint", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path) {
        try {
            long start = now();
            String result = mk.getJournal(fromRevisionId, toRevisionId, path);
            updateAndLogTimes("getJournal", start,
                    size(path), size(result));
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path, int depth) {
        try {
            long start = now();
            String result = mk.diff(fromRevisionId, toRevisionId, path, depth);
            updateAndLogTimes("diff", start, size(path), size(result));
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public long getLength(String blobId) {
        try {
            long start = now();
            long result = mk.getLength(blobId);
            updateAndLogTimes("getLength", start, size(blobId), 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes, String filter) {
        try {
            long start = now();
            String result = mk.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
            updateAndLogTimes("getNodes", start, size(path) + size(filter), size(result));
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) {
        try {
            long start = now();
            String result = mk.getRevisionHistory(since, maxEntries, path);
            updateAndLogTimes("getRevisionHistory", start, size(path), size(result));
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        try {
            long start = now();
            boolean result = mk.nodeExists(path, revisionId);
            updateAndLogTimes("nodeExists", start, size(path), 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        try {
            long start = now();
            long result = mk.getChildNodeCount(path, revisionId);
            updateAndLogTimes("getChildNodeCount", start, size(path), 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        try {
            long start = now();
            int result = mk.read(blobId, pos, buff, off, length);
            updateAndLogTimes("read", start, size(blobId) + length, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long maxWaitMillis) throws InterruptedException {
        try {
            long start = now();
            String result = mk.waitForCommit(oldHeadRevisionId, maxWaitMillis);
            updateAndLogTimes("waitForCommit", start, 0, 0);
            return result;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String write(InputStream in) {
        try {
            long start = now();
            String result = mk.write(in);
            updateAndLogTimes("write", start, 0, size(result));
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String branch(String trunkRevisionId) {
        try {
            long start = now();
            String result = mk.branch(trunkRevisionId);
            updateAndLogTimes("branch", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public String merge(String branchRevisionId, String message) {
        try {
            long start = now();
            String result = mk.merge(branchRevisionId, message);
            updateAndLogTimes("merge", start, size(message), 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Nonnull
    @Override
    public String rebase(@Nonnull String branchRevisionId, String newBaseRevisionId) {
        try {
            long start = now();
            String result = mk.rebase(branchRevisionId, newBaseRevisionId);
            updateAndLogTimes("rebase", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Nonnull
    @Override
    public String reset(@Nonnull String branchRevisionId,
                        @Nonnull String ancestorRevisionId)
            throws MicroKernelException {
        try {
            long start = now();
            String result = mk.reset(branchRevisionId, ancestorRevisionId);
            updateAndLogTimes("reset", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private static RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new MicroKernelException("Unexpected exception: " + e.toString(), e);
    }

    private void log(String message) {
        if (DEBUG) {
            System.out.println("[" + id + "] " + message);
        }
    }
    
    private static int size(String s) {
        return s == null ? 0 : s.length();
    }
    
    private static long now() {
        return System.currentTimeMillis();
    }

    private void updateAndLogTimes(String operation, long start, int paramSize, int resultSize) {
        long now = now();
        if (startTime == 0) {
            startTime = now;
        }
        Count c = counts.get(operation);
        if (c == null) {
            c = new Count();
            counts.put(operation, c);
        }
        c.update(now - start, paramSize, resultSize);
        long t = now - lastLogTime;
        if (t >= 2000) {
            totalLogTime += t;
            lastLogTime = now;
            long totalCount = 0, totalTime = 0;
            for (Count count : counts.values()) {
                totalCount += count.count;
                totalTime += count.total;
            }
            totalCount = Math.max(1, totalCount);
            totalTime = Math.max(1, totalTime);
            for (Entry<String, Count> e : counts.entrySet()) {
                c = e.getValue();
                long count = c.count;
                long total = c.total;
                long in = c.paramSize / 1024 / 1024;
                long out = c.resultSize / 1024 / 1024;
                if (count > 0) {
                    log(e.getKey() + 
                            " count " + count + 
                            " " + (100 * count / totalCount) + "%" +
                            " in " + in + " out " + out +
                            " time " + total +
                            " " + (100 * total / totalTime) + "%");
                }
            }
            log("all count " + totalCount + " time " + totalTime + " " + 
                    (100 * totalTime / totalLogTime) + "%");
            log("------");
        }
    }
}
