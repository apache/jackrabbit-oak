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
package org.apache.jackrabbit.mk.wrapper;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.jackrabbit.mk.MicroKernelFactory;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.util.ExceptionFactory;

/**
 * A logging microkernel implementation.
 */
public class LogWrapper implements MicroKernel {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("mk.debug", "true"));
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final MicroKernel mk;
    private final int id = NEXT_ID.getAndIncrement();

    public LogWrapper(MicroKernel mk) {
        this.mk = mk;
    }

    public static synchronized LogWrapper get(String url) {
        String u = url.substring("log:".length());
        LogWrapper w = new LogWrapper(MicroKernelFactory.getInstance(u));
        w.log("MicroKernel mk" + w.id + " = MicroKernelFactory.getInstance("
                + JsopBuilder.encode(u) + ");");
        return w;
    }

    public String commit(String path, String jsonDiff, String revisionId, String message) {
        try {
            logMethod("commit", path, jsonDiff, revisionId, message);
            String result = mk.commit(path, jsonDiff, revisionId, message);
            logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public void dispose() {
        try {
            logMethod("dispose");
            mk.dispose();
            logResult();
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String getHeadRevision() {
        try {
            logMethod("getHeadRevision");
            String result = mk.getHeadRevision();
            logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String getJournal(String fromRevisionId, String toRevisionId) {
        try {
            logMethod("getJournal", fromRevisionId, toRevisionId);
            String result = mk.getJournal(fromRevisionId, toRevisionId);
            logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String diff(String fromRevisionId, String toRevisionId, String path) {
        try {
            logMethod("diff", fromRevisionId, toRevisionId, path);
            String result = mk.diff(fromRevisionId, toRevisionId, path);
            logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public long getLength(String blobId) {
        try {
            logMethod("getLength", blobId);
            long result = mk.getLength(blobId);
            logResult(Long.toString(result));
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String getNodes(String path, String revisionId) {
        try {
            logMethod("getNodes", path, revisionId);
            String result = mk.getNodes(path, revisionId);
            logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String getNodes(String path, String revisionId, int depth, long offset, int count) {
        try {
            logMethod("getNodes", path, revisionId, depth, offset, count);
            String result = mk.getNodes(path, revisionId, depth, offset, count);
            logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String getRevisions(long since, int maxEntries) {
        try {
            logMethod("getRevisions", since, maxEntries);
            String result = mk.getRevisions(since, maxEntries);
            logResult(result);
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public boolean nodeExists(String path, String revisionId) {
        try {
            logMethod("nodeExists", path, revisionId);
            boolean result = mk.nodeExists(path, revisionId);
            logResult(Boolean.toString(result));
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public long getChildNodeCount(String path, String revisionId) {
        try {
            logMethod("getChildNodeCount", path, revisionId);
            long result = mk.getChildNodeCount(path, revisionId);
            logResult(Long.toString(result));
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        try {
            logMethod("read", blobId, pos, buff, off, length);
            int result = mk.read(blobId, pos, buff, off, length);
            logResult(Integer.toString(result));
            return result;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String waitForCommit(String oldHeadRevision, long maxWaitMillis) throws InterruptedException {
        try {
            logMethod("waitForCommit", oldHeadRevision, maxWaitMillis);
            String result = mk.waitForCommit(oldHeadRevision, maxWaitMillis);
            logResult(result);
            return result;
        } catch (InterruptedException e) {
            logException(e);
            throw e;
        } catch (Exception e) {
            logException(e);
            throw convert(e);
        }
    }

    public String write(InputStream in) {
        try {
            logMethod("write", in.toString());
            String result = mk.write(in);
            logResult(result);
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

    private RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        log("// unexpected exception type: " + e.getClass().getName());
        return ExceptionFactory.convert(e);
    }

    private void logException(Exception e) {
        log("// exception: " + e.toString());
    }

    private void logResult(Object result) {
        log("// " + quote(result));
    }

    private void logResult() {
        // ignored
    }

    private void log(String message) {
        if (DEBUG) {
            System.out.println(message);
        }
    }

}
