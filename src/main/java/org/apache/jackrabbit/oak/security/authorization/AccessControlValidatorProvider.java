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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlContext;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AccessControlValidatorProvider... TODO
 */
class AccessControlValidatorProvider implements ValidatorProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AccessControlValidatorProvider.class);

    public AccessControlValidatorProvider(AccessControlContext accessControlContext) {
        // TODO
    }

    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        return new AccessControlValidator();
    }
}
