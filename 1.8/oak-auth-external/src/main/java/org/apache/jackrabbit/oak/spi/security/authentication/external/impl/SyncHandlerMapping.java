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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

/**
 * Marker interface identifying classes that map a given
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler SyncHandler}
 * to an {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider ExternalIdentityProvider}
 * where both are identified by their name.
 *
 * @see org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager#getSyncHandler(String)
 * @see org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler#getName()
 * @see org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager#getProvider(String)
 * @see org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider#getName()
 * @see ExternalLoginModuleFactory
 */
public interface SyncHandlerMapping {

    /**
     * Name of the parameter that configures the name of the external identity provider.
     */
    String PARAM_IDP_NAME = "idp.name";

    /**
     * Name of the parameter that configures the name of the synchronization handler.
     */
    String PARAM_SYNC_HANDLER_NAME = "sync.handlerName";

}