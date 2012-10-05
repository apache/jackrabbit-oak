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
package org.apache.jackrabbit.oak.query;

import java.text.ParseException;
import java.util.HashMap;

import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.junit.Before;

/**
 * AbstractQueryTest...
 */
public abstract class AbstractQueryTest extends AbstractOakTest {

    protected CoreValueFactory vf;
    protected SessionQueryEngine qe;
    protected ContentSession session;
    protected Root root;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        session = createAdminSession();
        vf = session.getCoreValueFactory();
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
    }

    protected Result executeQuery(String statement, String language, HashMap<String, CoreValue> sv) throws ParseException {
        return qe.executeQuery(statement, language, Long.MAX_VALUE, 0, sv,
                session.getLatestRoot(), null);
    }

}