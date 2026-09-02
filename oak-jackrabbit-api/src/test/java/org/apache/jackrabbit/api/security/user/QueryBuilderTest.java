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
package org.apache.jackrabbit.api.security.user;

import javax.jcr.Value;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class QueryBuilderTest {

    /**
     * Test implementation of {@link QueryBuilder} that does <em>not</em>
     * override {@link QueryBuilder#setIndexTag(String)} and therefore relies on
     * the default method. Every other method flips {@link #invoked} so that a
     * test can assert whether the implementation was touched.
     */
    private static final class TestQueryBuilder implements QueryBuilder<String> {

        private boolean invoked = false;

        private String record() {
            invoked = true;
            return "";
        }

        boolean isInvoked() {
            return invoked;
        }

        @Override
        public void setSelector(Class<? extends Authorizable> selector) {
            invoked = true;
        }

        @Override
        public void setScope(String groupName, boolean declaredOnly) {
            invoked = true;
        }

        @Override
        public void setCondition(String condition) {
            invoked = true;
        }

        @Override
        public void setSortOrder(String propertyName, Direction direction, boolean ignoreCase) {
            invoked = true;
        }

        @Override
        public void setSortOrder(String propertyName, Direction direction) {
            invoked = true;
        }

        @Override
        public void setLimit(Value bound, long maxCount) {
            invoked = true;
        }

        @Override
        public void setLimit(long offset, long maxCount) {
            invoked = true;
        }

        // setIndexTag is intentionally NOT overridden -> the default applies.

        @Override
        public String nameMatches(String pattern) {
            return record();
        }

        @Override
        public String neq(String relPath, Value value) {
            return record();
        }

        @Override
        public String eq(String relPath, Value value) {
            return record();
        }

        @Override
        public String lt(String relPath, Value value) {
            return record();
        }

        @Override
        public String le(String relPath, Value value) {
            return record();
        }

        @Override
        public String gt(String relPath, Value value) {
            return record();
        }

        @Override
        public String ge(String relPath, Value value) {
            return record();
        }

        @Override
        public String exists(String relPath) {
            return record();
        }

        @Override
        public String like(String relPath, String pattern) {
            return record();
        }

        @Override
        public String contains(String relPath, String searchExpr) {
            return record();
        }

        @Override
        public String impersonates(String name) {
            return record();
        }

        @Override
        public String not(String condition) {
            return record();
        }

        @Override
        public String and(String condition1, String condition2) {
            return record();
        }

        @Override
        public String or(String condition1, String condition2) {
            return record();
        }
    }

    /**
     * The default {@link QueryBuilder#setIndexTag(String)} implementation must
     * be a no-op: calling it on an implementation that does not override it must
     * not call back into that implementation (in particular it must not reach
     * the backing query manager). This keeps the method backwards compatible
     * with implementations that have no notion of index tags.
     */
    @Test
    public void testSetIndexTagDefaultDoesNotCallImplementation() {
        TestQueryBuilder builder = new TestQueryBuilder();

        builder.setIndexTag("myTag");

        assertFalse("default setIndexTag must not invoke the QueryBuilder implementation", builder.isInvoked());
    }
}
