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
package org.apache.jackrabbit.oak.scalability.benchmarks.search;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for search scalability benchmarks.
 *
 */
public abstract class SearchScalabilityBenchmark extends ScalabilityBenchmark {
    protected static final Logger LOG = LoggerFactory.getLogger(SearchScalabilityBenchmark.class);    

    /**
     * Controls the max results retrieved after search
     */
    private static final int MAX_RESULTS = Integer.getInteger("maxResults", 100);

    @Override
    public void execute(Repository repository, Credentials credentials, ExecutionContext context) 
            throws Exception {
        Session session = repository.login(credentials);
        QueryManager qm;
        try {
            qm = session.getWorkspace().getQueryManager();
            search(qm, context);
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    protected void search(QueryManager qm, ExecutionContext context) throws RepositoryException {
        Query q = getQuery(qm, context);
        QueryResult r = q.execute();
        RowIterator it = r.getRows();
        for (int rows = 0; it.hasNext() && rows < MAX_RESULTS; rows++) {
            Node node = it.nextRow().getNode();
            LOG.debug(node.getPath());
        }
    }

    protected abstract Query getQuery(@Nonnull final QueryManager qm, ExecutionContext context) 
            throws RepositoryException;
}

