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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import org.apache.tomcat.jdbc.pool.interceptor.AbstractQueryReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This intercepter logs all failed queries with DEBUG level.
 */
public class RDBFailedQueryLogger extends AbstractQueryReport {

    private static final Logger LOG = LoggerFactory.getLogger(RDBFailedQueryLogger.class);

    @Override
    protected String reportFailedQuery(String query, Object[] args, String name, long start, Throwable t) {
        final String sql = super.reportFailedQuery(query, args, name, start, t);
        LOG.debug("Failed query: {}, args: {}, method name: {}", query, args, name, t);
        return sql;
    }

    @Override
    protected void prepareCall(String query, long time) {
    }

    @Override
    protected void prepareStatement(String query, long time) {
    }

    @Override
    public void closeInvoked() {
    }

}
