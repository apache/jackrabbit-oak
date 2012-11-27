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

import org.apache.jackrabbit.core.query.FulltextQueryTest;
import org.apache.jackrabbit.core.query.FulltextSQL2QueryTest;
import org.apache.jackrabbit.core.query.JoinTest;
import org.apache.jackrabbit.core.query.LimitAndOffsetTest;
import org.apache.jackrabbit.core.query.MixinTest;
import org.apache.jackrabbit.core.query.PathQueryNodeTest;
import org.apache.jackrabbit.core.query.SQL2NodeLocalNameTest;
import org.apache.jackrabbit.core.query.SQL2OffsetLimitTest;
import org.apache.jackrabbit.core.query.SQL2OrderByTest;
import org.apache.jackrabbit.core.query.SQL2OuterJoinTest;
import org.apache.jackrabbit.core.query.SQLTest;
import org.apache.jackrabbit.core.query.SkipDeletedNodesTest;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class QueryJcrTest extends TestCase {

    public static Test suite() {
        TestSuite suite = new ConcurrentTestSuite("Jackrabbit query tests");
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

        // FAILURES
        //
        // suite.addTestSuite(QueryResultTest.class); // OAK-484
        // suite.addTestSuite(OrderByTest.class); // OAK-466
        // suite.addTestSuite(ParentNodeTest.class); // OAK-309
        // suite.addTestSuite(ExcerptTest.class); // OAK-318
        // suite.addTestSuite(SimilarQueryTest.class); // OAK-319
        // suite.addTestSuite(DerefTest.class); // OAK-321
        // suite.addTestSuite(XPathAxisTest.class); // OAK-322
        // suite.addTestSuite(SQL2QueryResultTest.class); // OAK-323
        // suite.addTestSuite(SimpleQueryTest.class); // OAK-327
        // suite.addTestSuite(FnNameQueryTest.class); // OAK-328
        // suite.addTestSuite(UpperLowerCaseQueryTest.class); // OAK-329
        // suite.addTestSuite(SQL2PathEscapingTest.class); // OAK-481

        // NOT IMPLEMENTED
        //
        // suite.addTestSuite(ChildAxisQueryTest.class); // sns
        // suite.addTestSuite(SelectClauseTest.class); // sns
        // suite.addTestSuite(ShareableNodeTest.class); // ws#clone
        // suite.addTestSuite(VersionStoreQueryTest.class); // versioning

        // TOO JR SPECIFIC
        //
        // suite.addTestSuite(LimitedAccessQueryTest.class); // acls
        // suite.addTestSuite(SkipDeniedNodesTest.class); // acls

        return suite;
    }

}
