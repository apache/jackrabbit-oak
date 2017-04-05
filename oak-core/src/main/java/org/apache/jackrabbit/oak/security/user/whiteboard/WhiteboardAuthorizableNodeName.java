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
package org.apache.jackrabbit.oak.security.user.whiteboard;

import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;

/**
 * Dynamic {@link AuthorizableNodeName} based on the available
 * whiteboard services.
 */
public class WhiteboardAuthorizableNodeName
        extends AbstractServiceTracker<AuthorizableNodeName>
        implements AuthorizableNodeName {

    public WhiteboardAuthorizableNodeName() {
        super(AuthorizableNodeName.class);
    }

    @Nonnull
    @Override
    public String generateNodeName(@Nonnull String authorizableId) {
        List<AuthorizableNodeName> services = getServices();
        if (services.isEmpty()) {
            return AuthorizableNodeName.DEFAULT.generateNodeName(authorizableId);
        } else {
            return services.iterator().next().generateNodeName(authorizableId);
        }
    }
}
