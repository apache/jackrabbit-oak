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
package org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;

/**
 * A MicroKernel wrapper that can be used to log and also time MicroKernel calls.
 */
public class TimingWrapper implements MicroKernel {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("mk.debug", "true"));
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final MicroKernel mk;
    private final int id = NEXT_ID.getAndIncrement();

    private long startTime;
    private final Map<String, Count> counts = new HashMap<String, Count>();
    private long lastLogTime;

    private static class Count {
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
            //logMethod("commit", path, jsonDiff, revisionId, message);
            String result = mk.commit(path, jsonDiff, revisionId, message);
            //logResult(result);
            updateAndLogTimes("commit", start);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public void dispose() {
        // do nothing
    }

    @Override
    public String getHeadRevision() {
        try {
            //logMethod("getHeadRevision");
            long start = System.currentTimeMillis();
            String result = mk.getHeadRevision();
            updateAndLogTimes("getHeadRevision", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path) {
        try {
            //logMethod("getJournal", fromRevisionId, toRevisionId);
            long start = System.currentTimeMillis();
            String result = mk.getJournal(fromRevisionId, toRevisionId, path);
            updateAndLogTimes("getJournal", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path, int depth) {
        try {
            //logMethod("diff", fromRevisionId, toRevisionId, path);
            long start = System.currentTimeMillis();
            String result = mk.diff(fromRevisionId, toRevisionId, path, depth);
            updateAndLogTimes("diff", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public long getLength(String blobId) {
        try {
            //logMethod("getLength", blobId);
            long start = System.currentTimeMillis();
            long result = mk.getLength(blobId);
            updateAndLogTimes("getLength", start);
            //logResult(Long.toString(result));
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes, String filter) {
        try {
            //logMethod("getNodes", path, revisionId, depth, offset, maxChildNodes, filter);
            long start = System.currentTimeMillis();
            String result = mk.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
            updateAndLogTimes("getNodes", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) {
        try {
            //logMethod("getRevisionHistory", since, maxEntries, path);
            long start = System.currentTimeMillis();
            String result = mk.getRevisionHistory(since, maxEntries, path);
            updateAndLogTimes("getRevisionHistory", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        try {
            //logMethod("nodeExists", path, revisionId);
            long start = System.currentTimeMillis();
            boolean result = mk.nodeExists(path, revisionId);
            updateAndLogTimes("nodeExists", start);
            //logResult(Boolean.toString(result));
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        try {
            //logMethod("getChildNodeCount", path, revisionId);
            long start = System.currentTimeMillis();
            long result = mk.getChildNodeCount(path, revisionId);
            updateAndLogTimes("getChildNodeCount", start);
            //logResult(Long.toString(result));
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        try {
            //logMethod("read", blobId, pos, buff, off, length);
            long start = System.currentTimeMillis();
            int result = mk.read(blobId, pos, buff, off, length);
            //logResult(Integer.toString(result));
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
            //logMethod("waitForCommit", oldHeadRevisionId, maxWaitMillis);
            long start = System.currentTimeMillis();
            String result = mk.waitForCommit(oldHeadRevisionId, maxWaitMillis);
            updateAndLogTimes("waitForCommit", start);
            //logResult(result);
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
            //logMethod("write", in.toString());
            long start = System.currentTimeMillis();
            String result = mk.write(in);
            updateAndLogTimes("write", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String branch(String trunkRevisionId) {
        try {
            //logMethod("branch", trunkRevisionId);
            long start = System.currentTimeMillis();
            String result = mk.branch(trunkRevisionId);
            updateAndLogTimes("branch", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    @Override
    public String merge(String branchRevisionId, String message) {
        try {
            //logMethod("merge", branchRevisionId, message);
            long start = System.currentTimeMillis();
            String result = mk.merge(branchRevisionId, message);
            updateAndLogTimes("merge", start);
            //logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    private void logMethod(String methodName, Object... args) {
        StringBuilder buff = new StringBuilder("mk");
        buff.append(id).append('.').append(methodName).append('(');
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(quote(args[i]));
        }
        buff.append(");");
        log(buff.toString());
    }

    public static String quote(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof String) {
            return JsopBuilder.encode((String) o);
        }
        return o.toString();
    }

    private static RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        log("// unexpected exception type: " + e.getClass().getName());
        return ExceptionFactory.convert(e);
    }

    private static void logException(Exception e) {
        log("// exception: " + e.toString());
    }

    private static void logResult(Object result) {
        log("// " + quote(result));
    }

    private static void log(String message) {
        if (DEBUG) {
            System.out.println(message);
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
                if (c > 0) {
                    log(count.getKey() + " --> count:" + c + " max: " + max + " total: " + total);
                }
            }
            System.out.println("Time: " + ((now - startTime) / 1000L));
            System.out.println("------");
        }
    }
}
