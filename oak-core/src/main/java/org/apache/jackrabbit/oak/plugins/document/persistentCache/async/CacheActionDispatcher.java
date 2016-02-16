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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import static com.google.common.collect.Multimaps.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * An asynchronous buffer of the CacheAction objects. The buffer removes
 * {@link #ACTIONS_TO_REMOVE} oldest entries if the queue length is larger than
 * {@link #MAX_SIZE}.
 */
public class CacheActionDispatcher implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CacheActionDispatcher.class);

    /**
     * What's the length of the queue.
     */
    static final int MAX_SIZE = 1024;

    /**
     * How many actions remove once the queue is longer than {@link #MAX_SIZE}.
     */
    static final int ACTIONS_TO_REMOVE = 256;

    final BlockingQueue<CacheAction<?, ?>> queue = new ArrayBlockingQueue<CacheAction<?, ?>>(MAX_SIZE * 2);

    private volatile boolean isRunning = true;

    @Override
    public void run() {
        while (isRunning) {
            try {
                CacheAction<?, ?> action = queue.poll(10, TimeUnit.MILLISECONDS);
                if (action != null && isRunning) {
                    action.execute();
                }
            } catch (InterruptedException e) {
                LOG.debug("Interrupted the queue.poll()", e);
            }
        }
        applyInvalidateActions();
    }

    /**
     * Stop the processing.
     */
    public void stop() {
        isRunning = false;
    }

    /**
     * Adds the new action and cleans the queue if necessary.
     *
     * @param action to be added
     */
    synchronized void add(CacheAction<?, ?> action) {
        if (queue.size() >= MAX_SIZE) {
            cleanTheQueue();
        }
        queue.offer(action);
    }

    /**
     * Clean the queue and add a single invalidate action for all the removed entries. 
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void cleanTheQueue() {
        List<CacheAction> removed = removeOldest();
        for (Entry<CacheWriteQueue, Collection<CacheAction>> e : groupByOwner(removed).entrySet()) {
            CacheWriteQueue owner = e.getKey();
            Collection<CacheAction> actions = e.getValue();
            List<Object> affectedKeys = cancelAll(actions);
            owner.addInvalidate(affectedKeys);
        }
    }

    /**
     * Remove {@link #ACTIONS_TO_REMOVE} oldest actions.
     *
     * @return A list of removed items.
     */
    @SuppressWarnings("rawtypes")
    private List<CacheAction> removeOldest() {
        List<CacheAction> removed = new ArrayList<CacheAction>();
        while (queue.size() > MAX_SIZE - ACTIONS_TO_REMOVE) {
            CacheAction toBeCanceled = queue.poll();
            if (toBeCanceled == null) {
                break;
            } else {
                removed.add(toBeCanceled);
            }
        }
        return removed;
    }

    /**
     * Group passed actions by their owners.
     *
     * @param actions to be grouped
     * @return map in which owner is the key and assigned action list is the value
     */
    @SuppressWarnings("rawtypes")
    private static Map<CacheWriteQueue, Collection<CacheAction>> groupByOwner(List<CacheAction> actions) {
        return index(actions, new Function<CacheAction, CacheWriteQueue>() {
            @Override
            public CacheWriteQueue apply(CacheAction input) {
                return input.getOwner();
            }
        }).asMap();
    }

    /**
     * Cancel all passed actions.
     *
     * @param actions to cancel
     * @return list of affected keys
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static List<Object> cancelAll(Collection<CacheAction> actions) {
        List<Object> cancelledKeys = new ArrayList<Object>();
        for (CacheAction action : actions) {
            action.cancel();
            Iterables.addAll(cancelledKeys, action.getAffectedKeys());
        }
        return cancelledKeys;
    }

    @SuppressWarnings("rawtypes")
    private void applyInvalidateActions() {
        CacheAction action;
        do {
            action = queue.poll();
            if (action instanceof InvalidateCacheAction) {
                action.execute();
            }
        } while (action != null);
    }

}