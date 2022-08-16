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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Interface to mark properties or nodes located underneath a synchronized external identity as being protected (i.e.
 * prevent modification through regular JCR/Jackrabbit API outside of the regular synchronization during login (or through 
 * JMX).
 */
@ProviderType
public interface ProtectionConfig {
    
    boolean isProtectedProperty(@NotNull Tree parent, @NotNull PropertyState property);
    
    boolean isProtectedTree(@NotNull Tree tree);

    /**
     * Default implementation that marks all properties or nodes as protected.
     */
    ProtectionConfig DEFAULT = new ProtectionConfig() {
        @Override
        public boolean isProtectedProperty(@NotNull Tree parent, @NotNull PropertyState property) {
            return true;
        }

        @Override
        public boolean isProtectedTree(@NotNull Tree tree) {
            return true;
        }
    };
}