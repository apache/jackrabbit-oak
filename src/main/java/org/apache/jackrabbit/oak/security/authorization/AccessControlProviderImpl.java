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
package org.apache.jackrabbit.oak.security.authorization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlContext;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlProvider;

/**
 * {@code AccessControlProviderImpl} is a default implementation and
 * creates {@link AccessControlContextImpl} for a given set of principals.
 */
public class AccessControlProviderImpl extends SecurityConfiguration.Default implements AccessControlProvider {

    @Override
    public AccessControlContext getAccessControlContext(Subject subject) {
        return new AccessControlContextImpl(subject);
    }

    @Override
    public List<ValidatorProvider> getValidatorProviders() {
        List<ValidatorProvider> vps = new ArrayList<ValidatorProvider>();
        vps.add(new PermissionValidatorProvider());
        vps.add(new AccessControlValidatorProvider());
        return Collections.unmodifiableList(vps);
    }
}
