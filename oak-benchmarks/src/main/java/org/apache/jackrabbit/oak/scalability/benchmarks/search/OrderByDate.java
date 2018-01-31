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
package org.apache.jackrabbit.oak.scalability.benchmarks.search;

import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityBlobSearchSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderByDate extends SearchScalabilityBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(OrderByDate.class);
    
    @Override
    protected Query getQuery(final QueryManager qm, final ExecutionContext context) throws RepositoryException {
        final String path = (String) context.getMap().get(
            ScalabilityBlobSearchSuite.CTX_ROOT_NODE_NAME_PROP);
        final String statement = String.format(
            "SELECT * FROM [%s] WHERE ISDESCENDANTNODE('/%s') ORDER BY [jcr:content/jcr:lastModified]",
            context.getMap().get(ScalabilityBlobSearchSuite.CTX_FILE_NODE_TYPE_PROP),
            path);
        
        LOG.debug("statement: {}", statement);
        
        return qm.createQuery(statement, Query.JCR_SQL2);
    }
}
