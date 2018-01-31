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

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.*;

import org.apache.jackrabbit.oak.scalability.suites.ScalabilityBlobSearchSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;

/**
 * Splits the query in {@link org.apache.jackrabbit.oak.scalability.benchmarks.search.MultiFilterOrderBySearcher}
 * into multiple queries and unions the results.
 */
public class MultiFilterSplitOrderBySearcher extends MultiFilterOrderBySearcher {
    @Override
    protected void search(QueryManager qm, ExecutionContext context)
        throws RepositoryException {
        searchCommon(qm, context);

        Query q = getQuery(qm, context);
        QueryResult r = q.execute();
        RowIterator it = r.getRows();

        for (int rows = 0; it.hasNext() && rows < LIMIT; rows++) {
            Node node = it.nextRow().getNode();
            LOG.debug(node.getPath());
        }
    }

    protected void searchCommon(QueryManager qm, ExecutionContext
        context) throws RepositoryException {
        /** Execute standard query */
        Query stdQuery = getStandardQuery(qm, context);
        stdQuery.setLimit(LIMIT);
        QueryResult stdResult = stdQuery.execute();
        RowIterator stdIt = stdResult.getRows();

        // Iterate the standard shown first
        for (int rows = 0; stdIt.hasNext() && rows < LIMIT; rows++) {
            Node node = stdIt.nextRow().getNode();
            LOG.debug(node.getPath());
        }
    }

    protected Query getStandardQuery(@Nonnull final QueryManager qm, ExecutionContext context)
        throws RepositoryException {
        // /jcr:root/LongevitySearchAssets/12345//element(*, ParentType)[(@viewed = 'true')] order
        // by @viewed descending
        StringBuilder statement = new StringBuilder("/jcr:root/");

        statement.append(
            ((String) context.getMap().get(ScalabilityBlobSearchSuite.CTX_ROOT_NODE_NAME_PROP)))
            .append("//element(*, ")
            .append(context.getMap().get(ScalabilityNodeSuite.CTX_ACT_NODE_TYPE_PROP)).append(")");
        statement.append("[(").append("@").append(ScalabilityNodeSuite.SORT_PROP)
            .append("= 'true'");
        statement.append(")]");

        LOG.debug("{}", statement);

        return qm.createQuery(statement.toString(), Query.XPATH);
    }

    @Override
    protected String getOrderByClause() {
        return " order by" + " @" + ScalabilityNodeSuite.DATE_PROP + " descending";
    }
}
