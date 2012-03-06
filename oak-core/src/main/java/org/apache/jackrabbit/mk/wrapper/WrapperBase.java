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
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;

public abstract class WrapperBase implements MicroKernel, Wrapper {

    public final String commit(String path, String jsonDiff, String revisionId, String message) {
        return commitStream(path, new JsopTokenizer(jsonDiff), revisionId, message);
    }

    public final String getJournal(String fromRevisionId, String toRevisionId, String filter) {
        return getJournalStream(fromRevisionId, toRevisionId, filter).toString();
    }

    public final String getNodes(String path, String revisionId) {
        return getNodesStream(path, revisionId).toString();
    }

    public final String getNodes(String path, String revisionId, int depth, long offset, int count, String filter) {
        return getNodesStream(path, revisionId, depth, offset, count, filter).toString();
    }

    public final String diff(String fromRevisionId, String toRevisionId, String filter) {
        return diffStream(fromRevisionId, toRevisionId, filter).toString();
    }

    public final String getRevisions(long since, int maxEntries) {
        return getRevisionsStream(since, maxEntries).toString();
    }

    public static Wrapper wrap(final MicroKernel mk) {
        if (mk instanceof Wrapper) {
            return (Wrapper) mk;
        }
        return new Wrapper() {

            MicroKernel wrapped = mk;

            public String commitStream(String path, JsopReader jsonDiff, String revisionId, String message) {
                return wrapped.commit(path, jsonDiff.toString(), revisionId, message);
            }

            public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String filter) {
                return new JsopTokenizer(wrapped.getJournal(fromRevisionId, toRevisionId, filter));
            }

            public JsopReader getNodesStream(String path, String revisionId) {
                return new JsopTokenizer(wrapped.getNodes(path, revisionId));
            }

            public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
                return new JsopTokenizer(wrapped.getNodes(path, revisionId, depth, offset, count, filter));
            }

            public JsopReader getRevisionsStream(long since, int maxEntries) {
                return new JsopTokenizer(wrapped.getRevisions(since, maxEntries));
            }

            public JsopReader diffStream(String fromRevisionId, String toRevisionId, String path) {
                return new JsopTokenizer(wrapped.diff(fromRevisionId, toRevisionId, path));
            }

            public String commit(String path, String jsonDiff, String revisionId, String message) {
                return wrapped.commit(path, jsonDiff, revisionId, message);
            }

            public String diff(String fromRevisionId, String toRevisionId, String path) {
                return wrapped.diff(fromRevisionId, toRevisionId, path);
            }

            public void dispose() {
                wrapped.dispose();
            }

            public String getHeadRevision() throws MicroKernelException {
                return wrapped.getHeadRevision();
            }

            public String getJournal(String fromRevisionId, String toRevisionId, String filter) {
                return wrapped.getJournal(fromRevisionId, toRevisionId, filter);
            }

            public long getLength(String blobId) {
                return wrapped.getLength(blobId);
            }

            public String getNodes(String path, String revisionId) {
                return wrapped.getNodes(path, revisionId);
            }

            public String getNodes(String path, String revisionId, int depth, long offset, int count, String filter) {
                return wrapped.getNodes(path, revisionId, depth, offset, count, filter);
            }

            public String getRevisions(long since, int maxEntries) {
                return wrapped.getRevisions(since, maxEntries);
            }

            public boolean nodeExists(String path, String revisionId) {
                return wrapped.nodeExists(path, revisionId);
            }

            public long getChildNodeCount(String path, String revisionId) {
                return wrapped.getChildNodeCount(path, revisionId);
            }

            public int read(String blobId, long pos, byte[] buff, int off, int length) {
                return wrapped.read(blobId, pos, buff, off, length);
            }

            public String waitForCommit(String oldHeadRevision, long maxWaitMillis) throws InterruptedException {
                return wrapped.waitForCommit(oldHeadRevision, maxWaitMillis);
            }

            public String write(InputStream in) {
                return wrapped.write(in);
            }

        };
    }

}
