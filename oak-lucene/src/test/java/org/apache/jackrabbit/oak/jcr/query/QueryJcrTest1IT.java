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
package org.apache.jackrabbit.oak.jcr.query;

import org.apache.jackrabbit.core.query.ExcerptTest;
import org.apache.jackrabbit.core.query.FulltextQueryTest;
import org.apache.jackrabbit.core.query.FulltextSQL2QueryTest;
import org.apache.jackrabbit.core.query.JoinTest;
import org.apache.jackrabbit.core.query.LimitAndOffsetTest;
import org.apache.jackrabbit.core.query.MixinTest;
import org.apache.jackrabbit.core.query.OrderByTest;
import org.apache.jackrabbit.core.query.PathQueryNodeTest;
import org.apache.jackrabbit.core.query.QueryResultTest;
import org.apache.jackrabbit.core.query.SQL2NodeLocalNameTest;
import org.apache.jackrabbit.core.query.SQL2OffsetLimitTest;
import org.apache.jackrabbit.core.query.SQL2OrderByTest;
import org.apache.jackrabbit.core.query.SQL2OuterJoinTest;
import org.apache.jackrabbit.core.query.SQLTest;
import org.apache.jackrabbit.core.query.SkipDeletedNodesTest;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class QueryJcrTest1IT extends TestCase {

    public static Test suite() {
        TestSuite suite = new ConcurrentTestSuite(
                "Jackrabbit query tests using a Lucene based index");
        suite.addTestSuite(FulltextQueryTest.class);
        suite.addTestSuite(SQLTest.class);
        suite.addTestSuite(JoinTest.class);
        suite.addTestSuite(SkipDeletedNodesTest.class);
        suite.addTestSuite(PathQueryNodeTest.class);
        suite.addTestSuite(FulltextSQL2QueryTest.class);
        suite.addTestSuite(SQL2NodeLocalNameTest.class);
        suite.addTestSuite(SQL2OrderByTest.class);
        suite.addTestSuite(MixinTest.class);
        suite.addTestSuite(SQL2OuterJoinTest.class);
        suite.addTestSuite(SQL2OffsetLimitTest.class);
        suite.addTestSuite(LimitAndOffsetTest.class);
        suite.addTestSuite(OrderByTest.class);
        suite.addTestSuite(ExcerptTest.class);
        suite.addTestSuite(QueryResultTest.class);
        return suite;
    }
}
