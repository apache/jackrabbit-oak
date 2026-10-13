/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.query.SessionQuerySettingsProviderService;
import org.apache.jackrabbit.oak.spi.query.SessionQuerySettingsProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import java.lang.reflect.Method;
import java.util.Collections;

/**
 * Same result-size assertions as {@link ResultSizeCommonTest}, but the fast (insecure)
 * count is gated by a {@code SessionQuerySettingsProvider} registered on the Whiteboard
 * and reconfigured per principal, instead of the
 * {@code oak.fastQuerySize} system property. Ported from the Lucene-only
 * {@code org.apache.jackrabbit.oak.jcr.query.WhiteboardResultSizeTest}.
 */
public abstract class WhiteboardResultSizeCommonTest extends ResultSizeCommonTest {

    @SessionQuerySettingsProviderService.Configuration(directCountsPrincipals = {"admin"})
    static class AdminAllowed {
    }

    @SessionQuerySettingsProviderService.Configuration
    static class NoneAllowed {
    }

    protected final SessionQuerySettingsProviderService settingsProviderService =
            new SessionQuerySettingsProviderService();
    private Repository repository;

    /** Register the settings provider on the whiteboard before creating the repository. */
    protected Repository buildRepository(Oak oak) {
        Whiteboard whiteboard = oak.getWhiteboard();
        whiteboard.register(SessionQuerySettingsProvider.class, settingsProviderService, Collections.emptyMap());
        repository = new Jcr(oak).createRepository();
        return repository;
    }

    private void reconfigure(Class<?> configHolder) {
        try {
            Method m = SessionQuerySettingsProviderService.class
                    .getDeclaredMethod("configure", SessionQuerySettingsProviderService.Configuration.class);
            m.setAccessible(true);
            m.invoke(settingsProviderService,
                    configHolder.getAnnotation(SessionQuerySettingsProviderService.Configuration.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void setDirectResultCount(boolean fast) {
        reconfigure(fast ? AdminAllowed.class : NoneAllowed.class);
    }

    @Override
    protected Session querySession() throws RepositoryException {
        // provider settings are captured at login -> re-login to pick up the reconfiguration
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
    }

    @Override
    protected void releaseQuerySession(Session session) {
        if (session != null) {
            session.logout();
        }
    }
}
