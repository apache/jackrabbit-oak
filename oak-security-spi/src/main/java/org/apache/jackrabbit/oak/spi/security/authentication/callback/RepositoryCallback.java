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
package org.apache.jackrabbit.oak.spi.security.authentication.callback;

import javax.annotation.CheckForNull;
import javax.security.auth.callback.Callback;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

/**
 * Callback implementation used to access the repository. It allows to set and
 * get the {@code ContentRepository} and the name of the workspace for which
 * the login applies. In addition it provides access to a {@link SecurityProvider}
 * associated with the content repository.
 */
public class RepositoryCallback implements Callback {

    private ContentRepository contentRepository;
    private SecurityProvider securityProvider;
    private String workspaceName;

    @CheckForNull
    public String getWorkspaceName() {
        return workspaceName;
    }

    public void setWorkspaceName(String workspaceName) {
        this.workspaceName = workspaceName;
    }

    @CheckForNull
    public ContentRepository getContentRepository() {
        return contentRepository;
    }

    public void setContentRepository(ContentRepository contentRepository) {
        this.contentRepository = contentRepository;
    }

    @CheckForNull
    public SecurityProvider getSecurityProvider() {
        return securityProvider;
    }

    public void setSecurityProvider(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }
}
