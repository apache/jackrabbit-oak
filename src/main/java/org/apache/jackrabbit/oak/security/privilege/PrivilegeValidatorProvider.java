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
package org.apache.jackrabbit.oak.security.privilege;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.commit.SubtreeValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.REP_PRIVILEGES;

/**
 * PrivilegeValidatorProvider... TODO
 */
public class PrivilegeValidatorProvider implements ValidatorProvider {

    private final ContentSession contentSession = null; // TODO

    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        try {
            // TODO check again...
            return new SubtreeValidator(new PrivilegeRegistry(contentSession), JCR_SYSTEM, REP_PRIVILEGES);
        } catch (RepositoryException e) {
            throw new IllegalStateException(e);
        }
    }
}