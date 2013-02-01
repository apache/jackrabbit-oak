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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;

/**
 * A MicroKernel implementation that extends this interface can use a JsopReader
 * instead of having to use strings.
 */
public abstract class MicroKernelWrapperBase implements MicroKernel, MicroKernelWrapper {

    @Override
    public final String commit(String path, String jsonDiff, String revisionId, String message) {
        return commitStream(path, new JsopTokenizer(jsonDiff), revisionId, message);
    }

    @Override
    public final String getJournal(String fromRevisionId, String toRevisionId, String path) {
        return getJournalStream(fromRevisionId, toRevisionId, path).toString();
    }

    @Override
    public final String getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes, String filter) {
        JsopReader reader =
                getNodesStream(path, revisionId, depth, offset, maxChildNodes, filter);
        if (reader != null) {
            return reader.toString();
        } else {
            return null;
        }
    }

    @Override
    public final String diff(String fromRevisionId, String toRevisionId, String path, int depth) {
        return diffStream(fromRevisionId, toRevisionId, path, depth).toString();
    }

    @Override
    public final String getRevisionHistory(long since, int maxEntries, String path) {
        return getRevisionsStream(since, maxEntries, path).toString();
    }

    /**
     * Wrap a MicroKernel implementation so that the MicroKernelWrapper
     * interface can be used.
     *
     * @param mk the MicroKernel implementation to wrap
     * @return the wrapped instance
     */
    public static MicroKernelWrapper wrap(final MicroKernel mk) {
        if (mk instanceof MicroKernelWrapperImpl) {
            return (MicroKernelWrapper) mk;
        }
        return new MicroKernelWrapperImpl(mk);
    }

    /**
     * Unwrap a wrapped MicroKernel implementation previously created by
     * {@link #wrap};
     *
     * @param wrapper the MicroKernel wrapper to unwrap
     * @return the unwrapped instance
     * @throws IllegalArgumentException if the specified instance was not
     * originally returned by {@link #wrap}.
     */
    public static MicroKernel unwrap(final MicroKernelWrapper wrapper) {
        if (wrapper instanceof MicroKernelWrapperImpl) {
            return ((MicroKernelWrapperImpl) wrapper).getWrapped();
        }
        throw new IllegalArgumentException("wrapper instance was not created by this factory");
    }

    /**
     * A wrapper for MicroKernel implementations that don't support JSOP methods.
     */
    private static class MicroKernelWrapperImpl implements MicroKernelWrapper {

        final MicroKernel wrapped;

        MicroKernelWrapperImpl(MicroKernel mk) {
            wrapped = mk;
        }

        MicroKernel getWrapped() {
            return wrapped;
        }

        @Override
        public String commitStream(String path, JsopReader jsonDiff, String revisionId, String message) {
            return wrapped.commit(path, jsonDiff.toString(), revisionId, message);
        }

        @Override
        public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String path) {
            return new JsopTokenizer(wrapped.getJournal(fromRevisionId, toRevisionId, path));
        }

        @Override
        public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
            String json = wrapped.getNodes(
                    path, revisionId, depth, offset, count, filter);
            if (json != null) {
                return new JsopTokenizer(json);
            } else {
                return null;
            }
        }

        @Override
        public JsopReader getRevisionsStream(long since, int maxEntries, String path) {
            return new JsopTokenizer(wrapped.getRevisionHistory(since, maxEntries, path));
        }

        @Override
        public JsopReader diffStream(String fromRevisionId, String toRevisionId, String path, int depth) {
            return new JsopTokenizer(wrapped.diff(fromRevisionId, toRevisionId, path, depth));
        }

        @Override
        public String commit(String path, String jsonDiff, String revisionId, String message) {
            return wrapped.commit(path, jsonDiff, revisionId, message);
        }

        @Override
        public String branch(String trunkRevisionId) {
            return wrapped.branch(trunkRevisionId);
        }

        @Override
        public String merge(String branchRevisionId, String message) {
            return wrapped.merge(branchRevisionId, message);
        }

        @Nonnull
        @Override
        public String rebase(@Nonnull String branchRevisionId, String newBaseRevisionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String diff(String fromRevisionId, String toRevisionId, String path, int depth) {
            return wrapped.diff(fromRevisionId, toRevisionId, path, depth);
        }

        @Override
        public String getHeadRevision() throws MicroKernelException {
            return wrapped.getHeadRevision();
        }

        @Override
        public String getJournal(String fromRevisionId, String toRevisionId, String path) {
            return wrapped.getJournal(fromRevisionId, toRevisionId, path);
        }

        @Override
        public long getLength(String blobId) {
            return wrapped.getLength(blobId);
        }

        @Override
        public String getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes, String filter) {
            return wrapped.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
        }

        @Override
        public String getRevisionHistory(long since, int maxEntries, String path) {
            return wrapped.getRevisionHistory(since, maxEntries, path);
        }

        @Override
        public boolean nodeExists(String path, String revisionId) {
            return wrapped.nodeExists(path, revisionId);
        }

        @Override
        public long getChildNodeCount(String path, String revisionId) {
            return wrapped.getChildNodeCount(path, revisionId);
        }

        @Override
        public int read(String blobId, long pos, byte[] buff, int off, int length) {
            return wrapped.read(blobId, pos, buff, off, length);
        }

        @Override
        public String waitForCommit(String oldHeadRevisionId, long maxWaitMillis) throws InterruptedException {
            return wrapped.waitForCommit(oldHeadRevisionId, maxWaitMillis);
        }

        @Override
        public String write(InputStream in) {
            return wrapped.write(in);
        }

    }

}
