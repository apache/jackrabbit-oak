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

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import org.apache.jackrabbit.oak.benchmark.util.Date;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityBlobSearchSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;

/**
 * Searches on node with a filter property and orders the results by 2 properties 
 *
 */
public class MultiFilterOrderBySearcher extends PaginationEnabledSearcher {
    @SuppressWarnings("deprecation")
    @Override
    protected Query getQuery(@Nonnull QueryManager qm, ExecutionContext context)
        throws RepositoryException {
        // /jcr:root/LongevitySearchAssets/12345//element(*, ParentType)[(@filter = 'true' or not
        // (@filter)] order by @viewed descending, @added descending
        StringBuilder statement = new StringBuilder("/jcr:root/");

        statement.append(
            ((String) context.getMap().get(ScalabilityBlobSearchSuite.CTX_ROOT_NODE_NAME_PROP)))
            .append("//element(*, ")
            .append(context.getMap().get(ScalabilityNodeSuite.CTX_ACT_NODE_TYPE_PROP)).append(")");
        statement.append("[((").append("@").append(ScalabilityNodeSuite.FILTER_PROP)
            .append(" = 'true'").append(" or").append(" not(@")
            .append(ScalabilityNodeSuite.FILTER_PROP).append("))");
        if (context.getMap().containsKey(KEYSET_VAL_PROP)) {
            statement.append(" and @").append(ScalabilityNodeSuite.CTX_PAGINATION_KEY_PROP)
                .append(" < xs:dateTime('").append(
                Date.convertToISO_8601_2000((Calendar) context.getMap().get(KEYSET_VAL_PROP)))
                .append("')");
        }
        statement.append(")]").append(getOrderByClause());

        LOG.debug("{}", statement);

        return qm.createQuery(statement.toString(), Query.XPATH);
    }
}

