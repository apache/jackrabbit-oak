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

import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import org.apache.jackrabbit.oak.benchmark.util.Date;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityBlobSearchSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;

/**
 * perform searches using the {@code jcr:lastModified} and the provided timeframe
 */
public class LastModifiedSearcher extends SearchScalabilityBenchmark {
    private final Date timeframe;
    
    public LastModifiedSearcher(Date timeframe) {
        this.timeframe = timeframe;
    }
    
    @SuppressWarnings("deprecation")
    @Override
    protected Query getQuery(QueryManager qm, ExecutionContext context) throws RepositoryException {
        //  /jcr:root/content/dam//element(*, dam:Asset)[(jcr:content/@jcr:lastModified >= xs:dateTime('2013-05-09T09:44:01.403Z'))
        final String path = (String) context.getMap().get(ScalabilityBlobSearchSuite.CTX_ROOT_NODE_NAME_PROP);
        final String statement = "/jcr:root/" + path + "//element(*, "
                                 + context.getMap().get(ScalabilityBlobSearchSuite.CTX_FILE_NODE_TYPE_PROP)
                                 + ")[(jcr:content/@jcr:lastModified >= xs:dateTime('"
                                 + timeframe.toISO_8601_2000() + "'))]";
        
        LOG.debug("LastModifiedSearcher: {}", statement);
        
        return qm.createQuery(statement, Query.XPATH);
    }

    @Override
    public String toString() {
        String s = "::";
        
        switch(timeframe) {
        case LAST_2_HRS:
            s += "Hour";
            break;
        case LAST_24_HRS:
            s += "Day";
            break;
        case LAST_7_DAYS:
            s += "Week";
            break;
        case LAST_MONTH:
            s += "Month";
            break;
        case LAST_YEAR:
            s += "Year";
            break;
        default:
        }
        
        return super.toString() + s;
    }
}

