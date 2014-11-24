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
        suite.addTestSuite(JoinTest.class); // fail
        suite.addTestSuite(SkipDeletedNodesTest.class);
        suite.addTestSuite(PathQueryNodeTest.class);
        suite.addTestSuite(FulltextSQL2QueryTest.class); // fail
        suite.addTestSuite(SQL2NodeLocalNameTest.class); // fail
        suite.addTestSuite(SQL2OrderByTest.class); // fail
        suite.addTestSuite(MixinTest.class); // fail
        suite.addTestSuite(SQL2OuterJoinTest.class);
        suite.addTestSuite(SQL2OffsetLimitTest.class);
        suite.addTestSuite(LimitAndOffsetTest.class); // randomly failing
        suite.addTestSuite(OrderByTest.class); // fail
        suite.addTestSuite(ExcerptTest.class); // error unsupported
        suite.addTestSuite(QueryResultTest.class); // fail
        suite.addTestSuite(ParentNodeTest.class);  // fail
        suite.addTestSuite(SimilarQueryTest.class); // error unsupported
        suite.addTestSuite(DerefTest.class); // error
        suite.addTestSuite(XPathAxisTest.class); // fail and error
        suite.addTestSuite(SQL2QueryResultTest.class);
        suite.addTestSuite(SimpleQueryTest.class); // fail and error
        suite.addTestSuite(FnNameQueryTest.class); // fail
        suite.addTestSuite(UpperLowerCaseQueryTest.class); // fail
        suite.addTestSuite(SQL2PathEscapingTest.class); // fail and error
        suite.addTestSuite(ChildAxisQueryTest.class); // fail and error : javax.jcr.ItemExistsException: node3
        suite.addTestSuite(SelectClauseTest.class); // error : javax.jcr.ItemExistsException: node
        suite.addTestSuite(ShareableNodeTest.class); //not implemented
        suite.addTestSuite(VersionStoreQueryTest.class);
        return suite;
    }
}
