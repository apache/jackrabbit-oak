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

/**
 * Interface to obtain the name of the {@link java.security.Principal} from a
 * given {@link ExternalIdentityRef}. The aim of this interface is to prevent
 * potentially unnecessary round trips associated with the following sequence:
 *
 * <pre>
 *     String principalName = null;
 *     ExternalIdentity extId = externalIDP.getIdentity(externalIdentityRef);
 *     if (extId != null) {
 *         principalName = extid.getPrincipalName();
 *     }
 * </pre>
 *
 * This interface is expected to be implemented by {@link ExternalIdentityProvider}s,
 * that can deduce the principal name from the reference without the extra round trip.
 */
public interface PrincipalNameResolver {

    /**
     * Deduce the name of the {@link java.security.Principal} associated with the
     * {@link ExternalIdentity} represented by the given {@link ExternalIdentityRef}.
     *
     * @param externalIdentityRef A valid {@link ExternalIdentityRef} to an existing {@link ExternalIdentity}
     * @return The name of the {@link java.security.Principal} associated with the external identity referenced by the given {@code externalIdentityRef}.
     * @throws ExternalIdentityException If the reference is not valid or another error occurs.
     */
    @Nonnull
    String fromExternalIdentityRef(@Nonnull ExternalIdentityRef externalIdentityRef) throws ExternalIdentityException;
}