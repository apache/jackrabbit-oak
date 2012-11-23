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

import org.apache.jackrabbit.core.query.SQLTest;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class QueryJcrTest extends TestCase {

    public static Test suite() {
        TestSuite suite = new ConcurrentTestSuite(
                "Jackrabbit query tests using a Lucene based index");
        // suite.addTestSuite(FulltextQueryTest.class);
        // ok but I'm not too sure about
        // OAK-348 - fulltext tokenization

        suite.addTestSuite(SQLTest.class); // ok

        // suite.addTestSuite(SimpleQueryTest.class);
        // OAK-327 eq support
        // like pattern escaped
        // type support

        // suite.addTestSuite(UpperLowerCaseQueryTest.class);
        // OAK-329 type check should fail but doesn't

        // suite.addTestSuite(QueryResultTest.class);
        // OAK-308
        // type checks *probably* fail some of the tests
        // some orderby conditions

        // suite.addTestSuite(SQL2QueryResultTest.class);
        // OAK-323 - column names

        // suite.addTestSuite(SQL2OuterJoinTest.class); // ok
        // suite.addTestSuite(SkipDeletedNodesTest.class); // ok
        // suite.addTestSuite(PathQueryNodeTest.class); // ok
        // suite.addTestSuite(FulltextSQL2QueryTest.class); // ok
        // suite.addTestSuite(SQL2NodeLocalNameTest.class); // ok
        // suite.addTestSuite(SQL2OffsetLimitTest.class); // ok
        // suite.addTestSuite(SQL2OrderByTest.class); // ok
        // suite.addTestSuite(MixinTest.class);// ok
        // suite.addTestSuite(JoinTest.class); // ok

        //
        // NOT OK
        //
        // suite.addTestSuite(OrderByTest.class); // OAK-347
        // suite.addTestSuite(SQL2PathEscapingTest.class); // OAK-295

        // suite.addTestSuite(LimitAndOffsetTest.class); // OAK-308
        // suite.addTestSuite(ParentNodeTest.class); // OAK-309
        // suite.addTestSuite(XPathAxisTest.class); // OAK-322

        // suite.addTestSuite(FnNameQueryTest.class);
        // OAK-328: "%:content" illegal name

        //
        // NOT IMPLEMENTED
        //
        // suite.addTestSuite(DerefTest.class); // OAK-321
        // suite.addTestSuite(ExcerptTest.class); // OAK-318
        // suite.addTestSuite(SimilarQueryTest.class); // OAK-319
        // suite.addTestSuite(ChildAxisQueryTest.class); // sns
        // suite.addTestSuite(SelectClauseTest.class); // sns
        // suite.addTestSuite(ShareableNodeTest.class); // ws#clone
        // suite.addTestSuite(VersionStoreQueryTest.class); // versioning

        //
        // TOO JR SPECIFIC
        //
        // suite.addTestSuite(LimitedAccessQueryTest.class); // acls
        // suite.addTestSuite(SkipDeniedNodesTest.class); // acls

        return suite;
    }
}
