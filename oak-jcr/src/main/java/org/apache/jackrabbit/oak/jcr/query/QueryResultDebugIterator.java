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
package org.apache.jackrabbit.oak.jcr.query;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator which prints warnings when a certain number of elements were read from it.
 * @param <K>
 */

public class QueryResultDebugIterator<K> implements Iterator<K> {

    private static final Logger LOG = LoggerFactory.getLogger(QueryResultDebugIterator.class);

    private final Iterator<K> iter;
    private final String query;
    private final String queryLanguage;
    private int resultsRead;

    private static final int FIRST_LOG_THRESHOLD = 1_000;
    private static final int SECOND_LOG_THRESHOLD = 10_000;


    public QueryResultDebugIterator (Iterator<K> it, String query, String queryLanguage) {
        this.iter = it;
        this.query = query;
        this.queryLanguage = queryLanguage;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public K next() {
        resultsRead++;
        if (resultsRead >= FIRST_LOG_THRESHOLD) {
            potentiallyLog();
        }
        return iter.next();
    }

    private void potentiallyLog() {
        
        boolean shouldWarn = ((resultsRead == FIRST_LOG_THRESHOLD)
                || (resultsRead == SECOND_LOG_THRESHOLD));
        
        if (shouldWarn) {
            LOG.warn("Read {} results from result set of query='{}', query language='{}')", resultsRead, query, queryLanguage);
        }
        if (resultsRead > SECOND_LOG_THRESHOLD && resultsRead % 1000 == 0) {
            LOG.trace("Read {} results from result set of query='{}', query language='{}')", resultsRead, query, queryLanguage);
        }
    }


}
