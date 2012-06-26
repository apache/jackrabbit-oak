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
package org.apache.jackrabbit.oak.core;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.index.Indexer;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceValidatorProvider;
import org.apache.jackrabbit.oak.plugins.type.TypeValidatorProvider;
import org.apache.jackrabbit.oak.plugins.value.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.security.authentication.LoginContextProviderImpl;
import org.apache.jackrabbit.oak.spi.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingEditor;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MicroKernel}-based implementation of
 * the {@link ContentRepository} interface.
 */
public class ContentRepositoryImpl implements ContentRepository {

    /** Logger instance */
    private static final Logger LOG = LoggerFactory.getLogger(ContentRepositoryImpl.class);

    // TODO: retrieve default wsp-name from configuration
    private static final String DEFAULT_WORKSPACE_NAME = "default";

    private final LoginContextProvider loginContextProvider;

    private final QueryEngine queryEngine;
    private final NodeStore nodeStore;

    /**
     * Utility constructor that creates a new in-memory repository with default
     * query index provider. This constructor is intended to be used within
     * test cases only.
     */
    public ContentRepositoryImpl() {
        this(new MicroKernelImpl(), null, createDefaultValidatorProvider());
        // this(new IndexWrapper(new MicroKernelImpl()), null, null);
    }

    /**
     * Creates an Oak repository instance based on the given, already
     * initialized components.
     *
     * @param microKernel underlying kernel instance
     * @param indexProvider index provider
     * @param validatorProvider the validation provider
     */
    public ContentRepositoryImpl(MicroKernel microKernel, QueryIndexProvider indexProvider,
            ValidatorProvider validatorProvider) {

        if (validatorProvider == null) {
            validatorProvider = createDefaultValidatorProvider();
        }

        List<CommitEditor> editors = new ArrayList<CommitEditor>();
        editors.add(new ValidatingEditor(validatorProvider));
        CompositeEditor editor = new CompositeEditor(editors);

        nodeStore = new KernelNodeStore(microKernel, editor);
        QueryIndexProvider qip = (indexProvider == null) ? getDefaultIndexProvider(microKernel) : indexProvider;
        queryEngine = new QueryEngineImpl(nodeStore, microKernel, qip);

        // TODO: use configurable context provider
        loginContextProvider = new LoginContextProviderImpl(this);

        // FIXME: depends on CoreValue's name mangling
        String ntUnstructured = "nam:nt:unstructured";

        // FIXME: workspace setup must be done elsewhere...
        microKernel.commit("/",
                "^\"jcr:primaryType\":\"" + ntUnstructured + "\" ",
                null, null);
    }

    private static ValidatorProvider createDefaultValidatorProvider() {
        List<ValidatorProvider> providers = new ArrayList<ValidatorProvider>();
        providers.add(new NameValidatorProvider());
        providers.add(new NamespaceValidatorProvider());
        providers.add(new TypeValidatorProvider());
        providers.add(new ConflictValidatorProvider());
        return new CompositeValidatorProvider(providers);
    }

    private static QueryIndexProvider getDefaultIndexProvider(MicroKernel mk) {
        return Indexer.getInstance(mk);
    }

    @Nonnull
    @Override
    public ContentSession login(Credentials credentials, String workspaceName)
            throws LoginException, NoSuchWorkspaceException {
        if (workspaceName == null) {
            workspaceName = DEFAULT_WORKSPACE_NAME;
        }

        // TODO: support multiple workspaces. See OAK-118
        if (!DEFAULT_WORKSPACE_NAME.equals(workspaceName)) {
            throw new NoSuchWorkspaceException(workspaceName);
        }

        LoginContext loginContext = loginContextProvider.getLoginContext(credentials, workspaceName);
        loginContext.login();

        return new ContentSessionImpl(loginContext, workspaceName, nodeStore, queryEngine);
    }

    //------------------------------------------------------------< ConflictValidator >---

}
