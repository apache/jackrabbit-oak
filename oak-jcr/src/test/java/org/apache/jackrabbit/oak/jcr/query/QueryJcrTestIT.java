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

import junit.extensions.TestSetup;
import junit.framework.Test;
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
import org.apache.jackrabbit.oak.jcr.tck.TCKBase;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class QueryJcrTestIT extends TCKBase {

    public QueryJcrTestIT() {
        super("Jackrabbit query tests");
    }

    public static Test suite() {
        return new TestSetup(new QueryJcrTestIT()) {
            @Override
            protected void setUp() throws Exception {
                System.setProperty("oak.queryMinPrefetch", "1000");
            }
            @Override
            protected void tearDown() throws Exception {
                System.clearProperty("oak.queryMinPrefetch");
            }
        };
    }

    @Override
    protected void addTests() {
        TestSuite tests = new ConcurrentTestSuite("Jackrabbit query tests");
        tests.addTestSuite(FulltextQueryTest.class);
        tests.addTestSuite(SQLTest.class);
        tests.addTestSuite(JoinTest.class);
        tests.addTestSuite(SkipDeletedNodesTest.class);
        tests.addTestSuite(PathQueryNodeTest.class);
        tests.addTestSuite(FulltextSQL2QueryTest.class);
        tests.addTestSuite(SQL2NodeLocalNameTest.class);
        tests.addTestSuite(SQL2OrderByTest.class);
        tests.addTestSuite(MixinTest.class);
        tests.addTestSuite(SQL2OuterJoinTest.class);
        tests.addTestSuite(SQL2OffsetLimitTest.class);
        tests.addTestSuite(LimitAndOffsetTest.class);
        tests.addTestSuite(OrderByTest.class);
        tests.addTestSuite(ExcerptTest.class);
        tests.addTestSuite(QueryResultTest.class);
        tests.addTestSuite(ParentNodeTest.class);
        tests.addTestSuite(SimilarQueryTest.class);
        tests.addTestSuite(DerefTest.class);
        tests.addTestSuite(XPathAxisTest.class);
        tests.addTestSuite(SQL2QueryResultTest.class);
        tests.addTestSuite(SimpleQueryTest.class);
        tests.addTestSuite(FnNameQueryTest.class);
        tests.addTestSuite(UpperLowerCaseQueryTest.class);
        tests.addTestSuite(SQL2PathEscapingTest.class);
        tests.addTestSuite(ChildAxisQueryTest.class);
        tests.addTestSuite(SelectClauseTest.class);
        tests.addTestSuite(ShareableNodeTest.class);
        tests.addTestSuite(VersionStoreQueryTest.class);
        addTest(tests);
    }
}
