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
package org.apache.jackrabbit.oak.kernel;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;

/**
 * {@code NodeStoreBranch} based on {@link MicroKernel} branching and merging.
 * This implementation keeps changes in memory up to a certain limit and writes
 * them back to the Microkernel branch when the limit is exceeded.
 */
public class KernelNodeStoreBranch extends
        AbstractNodeStoreBranch<KernelNodeStore, KernelNodeState> {

    /** Lock for coordinating concurrent merge operations */
    private final Lock mergeLock;

    private final BlobSerializer blobs = new BlobSerializer() {
        @Override
        public String serialize(Blob blob) {
            KernelBlob kernelBlob;
            if (blob instanceof KernelBlob) {
                kernelBlob = (KernelBlob) blob;
            } else {
                try {
                    kernelBlob = store.createBlob(blob.getNewStream());
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            return kernelBlob.getBinaryID();
        }
    };

    public KernelNodeStoreBranch(KernelNodeStore kernelNodeStore,
                                 ChangeDispatcher dispatcher,
                                 Lock mergeLock,
                                 KernelNodeState base) {
        super(kernelNodeStore, dispatcher, base);
        this.mergeLock = checkNotNull(mergeLock);
    }

    //----------------------< AbstractNodeStoreBranch >-------------------------

    @Override
    public KernelNodeState createBranch(KernelNodeState state) {
        return store.branch(state);
    }

    @Override
    public KernelNodeState getRoot() {
        return store.getRoot();
    }

    @Override
    protected KernelNodeState rebase(KernelNodeState branchHead,
                                     KernelNodeState base) {
        return store.rebase(branchHead, base);
    }

    @Override
    protected KernelNodeState merge(KernelNodeState branchHead,
                                    CommitInfo info)
            throws CommitFailedException {
        try {
            return store.merge(branchHead);
        } catch (MicroKernelException e) {
            throw new CommitFailedException(MERGE, 1,
                    "Failed to merge changes to the underlying MicroKernel", e);
        }
    }

    @Nonnull
    @Override
    protected KernelNodeState reset(@Nonnull KernelNodeState branchHead,
                                    @Nonnull KernelNodeState ancestor) {
        return store.reset(branchHead, ancestor);
    }

    @Override
    protected KernelNodeState persist(NodeState toPersist,
                                      KernelNodeState base,
                                      CommitInfo info) {
        JsopDiff diff = new JsopDiff(blobs);
        toPersist.compareAgainstBaseState(base, diff);
        return store.commit(diff.toString(), base);
    }

    @Override
    protected KernelNodeState copy(String source,
                                   String target,
                                   KernelNodeState base) {
        return store.commit("*\"" + source + "\":\"" + target + '"', base);
    }

    @Override
    protected KernelNodeState move(String source,
                                   String target,
                                   KernelNodeState base) {
        return store.commit(">\"" + source + "\":\"" + target + '"', base);
    }

//------------------------< NodeStoreBranch >-------------------------------

    @Nonnull
    @Override
    public NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
            throws CommitFailedException {
        mergeLock.lock();
        try {
            return super.merge(hook, info);
        } catch (MicroKernelException e) {
            throw new CommitFailedException(MERGE, 1,
                    "Failed to merge changes to the underlying MicroKernel", e);
        } finally {
            mergeLock.unlock();
        }
    }
}
