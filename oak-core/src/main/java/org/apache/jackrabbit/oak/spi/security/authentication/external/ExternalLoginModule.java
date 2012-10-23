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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.Collections;
import java.util.Set;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExternalLoginModule... TODO
 */
public abstract class ExternalLoginModule extends AbstractLoginModule {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ExternalLoginModule.class);

    public static final String PARAM_SYNC_MODE = "syncMode";
    public static final SyncMode DEFAULT_SYNC_MODE = SyncMode.DEFAULT_SYNC;

    private static final String PARAM_SYNC_HANDLER = "syncHandler";
    private static final String DEFAULT_SYNC_HANDLER = DefaultSyncHandler.class.getName();

    //------------------------------------------------< ExternalLoginModule >---
    /**
     * TODO
     *
     * @return
     */
    protected abstract boolean loginSucceeded();

    /**
     * TODO
     *
     * @return
     */
    protected abstract ExternalUser getExternalUser();

    /**
     * TODO
     *
     * @return
     * @throws SyncException
     */
    protected SyncHandler getSyncHandler() throws SyncException {
        String shClass = options.getConfigValue(PARAM_SYNC_HANDLER, DEFAULT_SYNC_HANDLER);
        Object syncHandler;
        try {
            syncHandler = Class.forName(shClass).newInstance();
        } catch (Exception e) {
            throw new SyncException("Error while getting SyncHandler:", e);
        }

        if (syncHandler.getClass().isAssignableFrom(SyncHandler.class)) {
            return (SyncHandler) syncHandler;
        } else {
            throw new SyncException("Invalid SyncHandler class configured: " + syncHandler.getClass().getName());
        }
    }

    //------------------------------------------------< AbstractLoginModule >---

    /**
     * Default implementation of the {@link #getSupportedCredentials()} method
     * that only lists {@link SimpleCredentials} as supported. Subclasses that
     * wish to support other or additional credential implementations should
     * override this method.
     *
     * @return An immutable set containing only the {@link SimpleCredentials} class.
     */
    @Override
    protected Set<Class> getSupportedCredentials() {
        Class scClass = SimpleCredentials.class;
        return Collections.singleton(scClass);
    }

    //--------------------------------------------------------< LoginModule >---

    /**
     * TODO
     *
     * @return
     * @throws LoginException
     */
    @Override
    public boolean commit() throws LoginException {
        if (!loginSucceeded()) {
            return false;
        }

        try {
            SyncHandler handler = getSyncHandler();
            Root root = getRoot();
            String smValue = options.getConfigValue(PARAM_SYNC_MODE, null);
            SyncMode syncMode;
            if (smValue == null) {
                syncMode = DEFAULT_SYNC_MODE;
            } else {
                syncMode = SyncMode.fromStrings(Text.explode(smValue, ',', false));
            }
            if (handler.initialize(getUserManager(), root, syncMode, options)) {
                handler.sync(getExternalUser());
                root.commit();
                return true;
            } else {
                log.warn("Failed to initialize sync handler.");
                return false;
            }
        } catch (SyncException e) {
            throw new LoginException("User synchronization failed: " + e);
        } catch (CommitFailedException e) {
            throw new LoginException("User synchronization failed: " + e);
        }
    }
}