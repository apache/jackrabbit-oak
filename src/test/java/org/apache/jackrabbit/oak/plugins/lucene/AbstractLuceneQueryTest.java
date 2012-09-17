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
package org.apache.jackrabbit.oak.plugins.lucene;

import static org.apache.jackrabbit.oak.spi.query.IndexUtils.DEFAULT_INDEX_HOME;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceValidatorProvider;
import org.apache.jackrabbit.oak.plugins.type.DefaultTypeEditor;
import org.apache.jackrabbit.oak.plugins.type.TypeValidatorProvider;
import org.apache.jackrabbit.oak.plugins.value.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.Before;

/**
 * base class for lucene search tests
 */
public abstract class AbstractLuceneQueryTest extends AbstractOakTest implements
        LuceneIndexConstants {

    protected static final String SQL2 = "JCR-SQL2";

    protected MicroKernel mk;
    protected ContentSession session;
    protected CoreValueFactory vf;
    protected SessionQueryEngine qe;
    protected Root root;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        session = createAdminSession();
        root = session.getLatestRoot();
        vf = session.getCoreValueFactory();
        qe = session.getQueryEngine();
    }

    @Override
    protected ContentRepository createRepository() {
        mk = new MicroKernelImpl();
        QueryIndexProvider indexer = new LuceneIndexProvider(DEFAULT_INDEX_HOME);
        QueryIndexProvider qip = new CompositeQueryIndexProvider(indexer);
        return new ContentRepositoryImpl(mk, qip, buildDefaultCommitHook());
    }

    private CommitHook buildDefaultCommitHook() {
        List<CommitHook> hooks = new ArrayList<CommitHook>();
        hooks.add(new DefaultTypeEditor());
        hooks.add(new ValidatingHook(createDefaultValidatorProvider()));
        hooks.add(new LuceneHook(DEFAULT_INDEX_HOME));
        return new CompositeHook(hooks);
    }

    private static ValidatorProvider createDefaultValidatorProvider() {
        List<ValidatorProvider> providers = new ArrayList<ValidatorProvider>();
        providers.add(new NameValidatorProvider());
        providers.add(new NamespaceValidatorProvider());
        providers.add(new TypeValidatorProvider());
        providers.add(new ConflictValidatorProvider());
        return new CompositeValidatorProvider(providers);
    }

    protected Result executeQuery(String statement) throws ParseException {
        return qe.executeQuery(statement, SQL2, Long.MAX_VALUE, 0, null, session.getLatestRoot(), null);
    }
}