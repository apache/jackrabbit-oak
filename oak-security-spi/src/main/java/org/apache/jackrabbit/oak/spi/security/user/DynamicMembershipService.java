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
package org.apache.jackrabbit.oak.spi.security.user;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.jetbrains.annotations.NotNull;

public interface DynamicMembershipService {

    /**
     * Returns in instance of {@link DynamicMembershipProvider} for the given root, user manager and name-path mapper.
     * 
     * @param root The root associated with the {@link DynamicMembershipProvider}
     * @param userManager The user manager associated with the {@link DynamicMembershipProvider}
     * @param namePathMapper The name-path mapper associated with the {@link DynamicMembershipProvider}
     * @return an new instance of {@link DynamicMembershipProvider}
     */
    @NotNull
    DynamicMembershipProvider getDynamicMembershipProvider(@NotNull Root root, @NotNull UserManager userManager, @NotNull NamePathMapper namePathMapper);
}
