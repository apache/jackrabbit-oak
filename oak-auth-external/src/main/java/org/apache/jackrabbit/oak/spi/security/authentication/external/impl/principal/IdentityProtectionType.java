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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_NONE;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN;

enum IdentityProtectionType {
    
    NONE(VALUE_PROTECT_EXTERNAL_IDENTITIES_NONE),
    WARN(VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN),
    PROTECTED(VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED);

    final String label;
    
    IdentityProtectionType(@NotNull String label) {
        this.label = label;
    }
    
    static IdentityProtectionType fromLabel(@NotNull String label) {
        switch (label) {
            case VALUE_PROTECT_EXTERNAL_IDENTITIES_NONE: return NONE;
            case VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN : return WARN;
            case VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED : return PROTECTED;
            default: throw new IllegalArgumentException("unsupported label "+label);
        }
    }
    
}