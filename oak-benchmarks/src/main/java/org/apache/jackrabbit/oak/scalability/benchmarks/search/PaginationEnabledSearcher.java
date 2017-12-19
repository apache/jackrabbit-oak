/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.scalability.benchmarks.search;

import java.util.Calendar;
import java.util.TimeZone;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;

/**
 * Abstract class which defines utility methods for processing results like 
 * pagination and no pagination. 
 *
 */
public abstract class PaginationEnabledSearcher extends SearchScalabilityBenchmark {
    /**
     * Pagination limit for one page
     */
    protected static final int LIMIT = Integer.getInteger("limit", 25);

    /**
     * Number of page accesses
     */
    protected static final int PAGES = Integer.getInteger("pages", 5);

    protected static final String KEYSET_VAL_PROP = "keysetval";

    protected void processResultsOffsetPagination(@Nonnull final QueryManager qm,
            ExecutionContext context) throws RepositoryException {
        for (int page = 0; page < PAGES; page++) {
            Query query = getQuery(qm, context);
            query.setLimit(LIMIT);
            query.setOffset(page * LIMIT);

            iterate(query);
        }
    }

    private Node iterate(Query query) throws RepositoryException {
        QueryResult r = query.execute();
        RowIterator it = r.getRows();
        Node last = null;

        while (it.hasNext()) {
            last = it.nextRow().getNode();
            LOG.debug(last.getPath());
        }
        return last;
    }

    protected void processResultsKeysetPagination(@Nonnull final QueryManager qm,
            ExecutionContext context) throws RepositoryException {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("GMT"));
        context.getMap().put(KEYSET_VAL_PROP, now);

        for (int page = 0; page < PAGES; page++) {
            Query query = getQuery(qm, context);
            query.setLimit(LIMIT);

            Node lastNode = iterate(query);
            if (lastNode != null) {
                Property prop =
                        lastNode.getProperty(ScalabilityNodeSuite.CTX_PAGINATION_KEY_PROP);
                context.getMap().put(KEYSET_VAL_PROP, prop.getDate());
            }
        }
        context.getMap().remove(KEYSET_VAL_PROP);
    }

    protected String getOrderByClause() {
        return " order by" + " @" + ScalabilityNodeSuite.SORT_PROP + " descending," + " @"
            + ScalabilityNodeSuite.DATE_PROP + " descending";
    }
}

