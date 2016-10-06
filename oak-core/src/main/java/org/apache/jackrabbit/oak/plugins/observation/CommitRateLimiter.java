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

package org.apache.jackrabbit.oak.plugins.observation;

import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@code CommitHook} can be used to block or delay commits for any length of time.
 * As long as commits are blocked this hook throws a {@code CommitFailedException}.
 */
public class CommitRateLimiter implements CommitHook {
    private static final Logger LOG = LoggerFactory.getLogger(CommitRateLimiter.class);
    private volatile boolean blockCommits;
    private volatile long delay;

    // the observation call depth of the current thread
    // (only updated by the current thread, so technically isn't necessary that
    // this is an AtomicInteger, but it's simpler to use it)
    private static ThreadLocal<AtomicInteger> NON_BLOCKING_LEVEL = 
            new ThreadLocal<AtomicInteger>();
    
    private static boolean EXCEPTION_ON_BLOCK = 
            Boolean.getBoolean("oak.commitRateLimiter.exceptionOnBlock");

    /**
     * Block any further commits until {@link #unblockCommits()} is called.
     */
    public void blockCommits() {
        blockCommits = true;
    }

    /**
     * Unblock blocked commits.
     */
    public void unblockCommits() {
        blockCommits = false;
    }

    public boolean getBlockCommits() {
        return blockCommits;
    }

    /**
     * Number of milliseconds to delay commits going through this hook.
     * If {@code 0}, any currently blocked commit will be unblocked.
     * @param delay  milliseconds
     */
    public void setDelay(long delay) {
        if (LOG.isTraceEnabled()) {
            if (this.delay != delay) {
                // only log if the delay has changed
                LOG.trace("setDelay: delay changed from " + this.delay + " to " + delay);
            }
        }
        this.delay = delay;
        if (delay == 0) {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
            throws CommitFailedException {
        if (blockCommits && isThreadBlocking()) {
            blockCommit();
        } else {
            delay();
        }
        return after;
    }
    
    public void blockCommit() throws CommitFailedException {
        if (EXCEPTION_ON_BLOCK) {
            throw new CommitFailedException(OAK, 1, "System busy. Try again later.");
        }
        synchronized (this) {
            try {
                while (getBlockCommits()) {
                    wait(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CommitFailedException(OAK, 2,
                        "Interrupted while waiting to commit", e);
            }
        }
    }

    protected void delay() throws CommitFailedException {
        if (delay > 0 && isThreadBlocking()) {
            synchronized (this) {
                try {
                    long t0 = Clock.ACCURATE.getTime();
                    long dt = delay;
                    while (delay > 0 && dt > 0) {
                        LOG.trace("delay: waiting {}ms (delay={}ms)", dt, delay);
                        wait(dt);
                        dt = dt - Clock.ACCURATE.getTime() + t0;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CommitFailedException(OAK, 2, "Interrupted while waiting to commit", e);
                }
            }
        }
    }

    /**
     * The current thread will now run code that must not be throttled or
     * blocked, such as processing events (EventListener.onEvent is going to be
     * called).
     */
    public void beforeNonBlocking() {
        AtomicInteger value = NON_BLOCKING_LEVEL.get();
        if (value == null) {
            value = new AtomicInteger(1);
            NON_BLOCKING_LEVEL.set(value);
        } else {
            value.incrementAndGet();
        }
    }

    /**
     * The current thread finished running code that must not be throttled or
     * blocked.
     */
    public void afterNonBlocking() {
        AtomicInteger value = NON_BLOCKING_LEVEL.get();
        if (value == null) {
            // TODO should not happen (log an error?)
        } else {
            value.decrementAndGet();
        }        
    }
    
    /**
     * Check whether the current thread is non-blocking.
     * 
     * @return whether thread thread is non-blocking
     */
    public boolean isThreadBlocking() {
        AtomicInteger value = NON_BLOCKING_LEVEL.get();
        // no delay while processing events
        return value == null || value.get() == 0;
    }

}
