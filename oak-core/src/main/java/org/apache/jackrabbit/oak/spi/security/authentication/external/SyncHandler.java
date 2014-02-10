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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;

/**
 * SyncHandler is used to sync users and groups from an external source an {@link ExternalUserProvider}.
 * One sync task always operates within a {@link SyncContext} and is associated with a {@link SyncConfig}.
 *
 * todo:
 * - cleanup expired authorizables ?
 *
 */
public interface SyncHandler {

    boolean initialize(@Nonnull UserManager userManager, @Nonnull Root root,
                       @Nonnull SyncMode mode,
                       @Nonnull ConfigurationParameters options) throws SyncException;

    boolean sync(@Nonnull ExternalUser externalUser) throws SyncException;
}