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

import static org.apache.jackrabbit.oak.plugins.lucene.LuceneIndexUtils.DEFAULT_INDEX_NAME;
import static org.apache.jackrabbit.oak.plugins.lucene.LuceneIndexUtils.createIndexNode;
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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndexFactory;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceValidatorProvider;
import org.apache.jackrabbit.oak.plugins.type.DefaultTypeEditor;
import org.apache.jackrabbit.oak.plugins.type.TypeValidatorProvider;
import org.apache.jackrabbit.oak.plugins.value.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingEditor;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.IndexManager;
import org.apache.jackrabbit.oak.spi.query.IndexManagerImpl;
import org.apache.jackrabbit.oak.spi.query.IndexUtils;
import org.junit.Before;

/**
 * base class for lucene search tests
 */
public abstract class AbstractLuceneQueryTest extends AbstractOakTest {

    protected static final String SQL2 = "JCR-SQL2";

    private static final String TEST_INDEX_NAME = DEFAULT_INDEX_NAME;

    protected MicroKernel mk;
    protected ContentSession session;
    protected CoreValueFactory vf;
    protected SessionQueryEngine qe;
    protected Root root;

    @Before
    public void before() throws Exception {
        super.before();
        session = createAdminSession();
        root = session.getCurrentRoot();
        vf = session.getCoreValueFactory();
        qe = session.getQueryEngine();
        cleanupIndexNode();

    }

    @Override
    protected ContentRepository createRepository() {
        mk = new MicroKernelImpl();
        return new ContentRepositoryImpl(mk, new LuceneIndexProvider(
                DEFAULT_INDEX_HOME), buildDefaultCommitEditor());
    }

    private CommitEditor buildDefaultCommitEditor() {
        IndexManager im = new IndexManagerImpl(IndexUtils.DEFAULT_INDEX_HOME,
                mk, new PropertyIndexFactory(), new LuceneIndexFactory());

        List<CommitEditor> editors = new ArrayList<CommitEditor>();
        editors.add(new DefaultTypeEditor());
        editors.add(new ValidatingEditor(createDefaultValidatorProvider()));
        editors.add(im);
        return new CompositeEditor(editors);
    }

    private static ValidatorProvider createDefaultValidatorProvider() {
        List<ValidatorProvider> providers = new ArrayList<ValidatorProvider>();
        providers.add(new NameValidatorProvider());
        providers.add(new NamespaceValidatorProvider());
        providers.add(new TypeValidatorProvider());
        providers.add(new ConflictValidatorProvider());
        return new CompositeValidatorProvider(providers);
    }

    /**
     * Recreates an empty index node, ready to be used in tests
     * 
     * @throws Exception
     */
    private void cleanupIndexNode() throws Exception {
        Tree index = root.getTree(DEFAULT_INDEX_HOME);
        if (index != null) {
            index = index.getChild(TEST_INDEX_NAME);
            if (index != null) {
                index.remove();
            }
        } else {
            index = root.getTree("/");
            for (String p : PathUtils.elements(DEFAULT_INDEX_HOME)) {
                if (index.hasChild(p)) {
                    index = index.getChild(p);
                } else {
                    index = index.addChild(p);
                }
            }
        }

        createIndexNode(root.getTree(DEFAULT_INDEX_HOME), TEST_INDEX_NAME, vf);
        root.commit(DefaultConflictHandler.OURS);
    }

    protected Result executeQuery(String statement) throws ParseException {
        return qe.executeQuery(statement, SQL2, Long.MAX_VALUE, 0, null, null);
    }
}