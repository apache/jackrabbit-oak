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

import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityBlobSearchSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;

/**
 * Full text query search
 *
 */
public class FullTextSearcher extends SearchScalabilityBenchmark {
    private final Random random = new Random(93);

    @SuppressWarnings("deprecation")
    @Override
    protected Query getQuery(@Nonnull final QueryManager qm, ExecutionContext context) throws RepositoryException {
        @SuppressWarnings("unchecked")
        List<String> paths = (List<String>) context.getMap().get(ScalabilityBlobSearchSuite.CTX_SEARCH_PATHS_PROP);
        
        return qm.createQuery("//*[jcr:contains(., '" + paths.get(random.nextInt(paths.size()))  + "File"
                + "*"
                + "')] ", Query.XPATH);
    }
}

