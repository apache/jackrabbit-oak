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

package org.apache.jackrabbit.oak.plugins.index.importer;


import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Lock implementation for single node setup like for SegmentNodeStore
 * It works by check async indexer status via IndexStatsMBean and
 * then aborting it if found to be running
 */
public class AbortingIndexerLock implements AsyncIndexerLock<SimpleToken> {
    public static final int TIMEOUT_SECONDS = 300;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AsyncIndexInfoService infoService;
    private final Clock clock;

    public AbortingIndexerLock(AsyncIndexInfoService infoService) {
        this(infoService, Clock.SIMPLE);
    }

    public AbortingIndexerLock(AsyncIndexInfoService infoService, Clock clock) {
        this.infoService = infoService;
        this.clock = clock;
    }

    @Override
    public SimpleToken lock(String asyncIndexerLane) {
        IndexStatsMBean mbean = getIndexStatsMBean(asyncIndexerLane);

        if (IndexStatsMBean.STATUS_RUNNING.equals(mbean.getStatus())){
            log.info("Aborting current indexing run of async indexer for lane [{}]", asyncIndexerLane);
        }

        mbean.abortAndPause();
        retry(mbean, TIMEOUT_SECONDS, 1000);
        log.info("Aborted and paused async indexer for lane [{}]", asyncIndexerLane);
        return new SimpleToken(asyncIndexerLane);
    }

    @Override
    public void unlock(SimpleToken token) {
        getIndexStatsMBean(token.laneName).resume();
        log.info("Resumed async indexer for lane [{}]", token.laneName);
    }

    private IndexStatsMBean getIndexStatsMBean(String asyncIndexerLane) {
        AsyncIndexInfo info = infoService.getInfo(asyncIndexerLane);
        checkNotNull(info, "No AsyncIndexInfo found for lane [%s]", asyncIndexerLane);
        IndexStatsMBean mbean = info.getStatsMBean();
        return checkNotNull(mbean, "No IndexStatsMBean associated with [%s]", asyncIndexerLane);
    }

    private void retry(IndexStatsMBean mbean, int timeoutSeconds, int intervalBetweenTriesMsec) {
        long timeout = clock.getTime() + timeoutSeconds * 1000L;
        while (clock.getTime() < timeout) {
            try {
                if (!IndexStatsMBean.STATUS_RUNNING.equals(mbean.getStatus())) {
                    return;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                int delta = (int) (timeout -  clock.getTime() / 1000);
                log.info("Async indexer for lane [{}] found to be running. Would wait for {} seconds " +
                        "more for it to stop", mbean.getName(), delta);
                Thread.sleep(intervalBetweenTriesMsec);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        throw new IllegalStateException("RetryLoop failed, condition is false after " + timeoutSeconds + " seconds");
    }
}

final class SimpleToken implements AsyncIndexerLock.LockToken {
    final String laneName;

    SimpleToken(String laneName) {
        this.laneName = laneName;
    }
}
