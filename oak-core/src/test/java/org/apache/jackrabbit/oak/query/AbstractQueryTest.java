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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndexFactory;
import org.apache.jackrabbit.oak.spi.query.IndexManager;
import org.apache.jackrabbit.oak.spi.query.IndexManagerImpl;
import org.apache.jackrabbit.oak.spi.query.IndexUtils;
import org.junit.Before;

/**
 * AbstractQueryTest...
 */
public abstract class AbstractQueryTest extends AbstractOakTest {

    protected MicroKernel mk;
    protected ContentSession session;
    protected CoreValueFactory vf;
    protected SessionQueryEngine qe;

    @Override
    protected ContentRepository createRepository() {
        mk = new MicroKernelImpl();
        IndexManager im = new IndexManagerImpl(IndexUtils.DEFAULT_INDEX_HOME,
                mk, new PropertyIndexFactory());
        return new ContentRepositoryImpl(mk, null, im);
    }

    @Before
    public void before() throws Exception {
        super.before();
        session = createGuestSession();
        vf = session.getCoreValueFactory();
        qe = session.getQueryEngine();
    }

}