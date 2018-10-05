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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.jackrabbit.core.query.ChildAxisQueryTest;
import org.apache.jackrabbit.core.query.DerefTest;
import org.apache.jackrabbit.core.query.ExcerptTest;
import org.apache.jackrabbit.core.query.FnNameQueryTest;
import org.apache.jackrabbit.core.query.FulltextQueryTest;
import org.apache.jackrabbit.core.query.FulltextSQL2QueryTest;
import org.apache.jackrabbit.core.query.JoinTest;
import org.apache.jackrabbit.core.query.LimitAndOffsetTest;
import org.apache.jackrabbit.core.query.MixinTest;
import org.apache.jackrabbit.core.query.OrderByTest;
import org.apache.jackrabbit.core.query.ParentNodeTest;
import org.apache.jackrabbit.core.query.PathQueryNodeTest;
import org.apache.jackrabbit.core.query.QueryResultTest;
import org.apache.jackrabbit.core.query.SQL2NodeLocalNameTest;
import org.apache.jackrabbit.core.query.SQL2OffsetLimitTest;
import org.apache.jackrabbit.core.query.SQL2OrderByTest;
import org.apache.jackrabbit.core.query.SQL2OuterJoinTest;
import org.apache.jackrabbit.core.query.SQL2PathEscapingTest;
import org.apache.jackrabbit.core.query.SQL2QueryResultTest;
import org.apache.jackrabbit.core.query.SQLTest;
import org.apache.jackrabbit.core.query.SelectClauseTest;
import org.apache.jackrabbit.core.query.ShareableNodeTest;
import org.apache.jackrabbit.core.query.SimilarQueryTest;
import org.apache.jackrabbit.core.query.SimpleQueryTest;
import org.apache.jackrabbit.core.query.SkipDeletedNodesTest;
import org.apache.jackrabbit.core.query.UpperLowerCaseQueryTest;
import org.apache.jackrabbit.core.query.VersionStoreQueryTest;
import org.apache.jackrabbit.core.query.XPathAxisTest;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

/**
 * Test suite of JCR tests to be run with Oak Solr index
 */
public class QueryJcrTest extends TestCase {

    public static Test suite() {
        TestSuite suite = new ConcurrentTestSuite(
                "Jackrabbit query tests using a Solr based index");
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
        suite.addTestSuite(ParentNodeTest.class);
        suite.addTestSuite(SimilarQueryTest.class);
        suite.addTestSuite(DerefTest.class);
        suite.addTestSuite(XPathAxisTest.class);
        suite.addTestSuite(SQL2QueryResultTest.class);
        suite.addTestSuite(SimpleQueryTest.class);
        suite.addTestSuite(FnNameQueryTest.class);
        suite.addTestSuite(UpperLowerCaseQueryTest.class);
        suite.addTestSuite(SQL2PathEscapingTest.class);
        suite.addTestSuite(ChildAxisQueryTest.class);
        suite.addTestSuite(SelectClauseTest.class);
        suite.addTestSuite(ShareableNodeTest.class);
        suite.addTestSuite(VersionStoreQueryTest.class);
        return suite;
    }
}
