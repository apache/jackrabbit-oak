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

package org.apache.jackrabbit.oak.plugins.index.property;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;

@Component
@Service(QueryIndexProvider.class)
public class OrderedPropertyIndexProvider implements QueryIndexProvider {

    private static final long DEFAULT_NO_INDEX_CACHE_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    /**
     * How often it should check for a new ordered property index.
     */
    private static long noIndexCacheTimeout = DEFAULT_NO_INDEX_CACHE_TIMEOUT;

    /**
     * The last time when it checked for the existence of an ordered property index AND could not find any.
     */
    private volatile long lastNegativeIndexCheck;

    @Override
    @Nonnull
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        return ImmutableList.<QueryIndex> of(new OrderedPropertyIndex(this));
    }

    /**
     * @return <code>true</code> if there may be any ordered indexes below the root path
     */
    boolean mayHaveRootIndexes() {
        return System.currentTimeMillis() - lastNegativeIndexCheck > noIndexCacheTimeout;
    }

    /**
     * Indicates whether or not there are ordered indexes below the root path
     *
     * @param hasRootIndexes
     */
    void indicateRootIndexes(boolean hasRootIndexes) {
        lastNegativeIndexCheck = hasRootIndexes ? 0 : System.currentTimeMillis();
    }

    public static void setCacheTimeoutForTesting(long timeout) {
        noIndexCacheTimeout = timeout;
    }

    public static void resetCacheTimeoutForTesting() {
        noIndexCacheTimeout = DEFAULT_NO_INDEX_CACHE_TIMEOUT;
    }

}
