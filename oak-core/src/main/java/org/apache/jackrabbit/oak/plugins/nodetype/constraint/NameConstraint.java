/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.nodetype.constraint;

import java.util.function.Predicate;

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.core.GuavaDeprecation;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameConstraint implements Predicate<Value>, com.google.common.base.Predicate<Value> {
    private static final Logger log = LoggerFactory.getLogger(NameConstraint.class);

    private final String requiredValue;

    public NameConstraint(String definition) {
        requiredValue = definition;
    }

    @Override
    public boolean test(@Nullable Value value) {
        try {
            return value != null && requiredValue != null && requiredValue.equals(value.getString());
        }
        catch (RepositoryException e) {
            log.warn("Error checking name constraint " + this, e);
            return false;
        }
    }

    /**
     * @deprecated use {@link #test(Value)} instead  (see <a href="https://issues.apache.org/jira/browse/OAK-8874">OAK-8874</a>)
     */
    @Deprecated
    @Override
    public boolean apply(@Nullable Value value) {
        GuavaDeprecation.handleCall("OAK-8874");
        return test(value);
    }

    @Override
    public String toString() {
        return '\'' + requiredValue + '\'';
    }
}
