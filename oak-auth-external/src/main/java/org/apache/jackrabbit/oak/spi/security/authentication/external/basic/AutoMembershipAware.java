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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.jetbrains.annotations.NotNull;

/**
 * Interface to identify a given {@link SyncHandler} as being aware of the optional {@link AutoMembershipConfig}.
 */
public interface AutoMembershipAware {

    /**
     * Returns the {@link AutoMembershipConfig} defined with the {@link SyncHandler} implementing this interface.
     * 
     * @return an instance of {@code AutoMembershipConfig}
     */
    @NotNull AutoMembershipConfig getAutoMembershipConfig();
}