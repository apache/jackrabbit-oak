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

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BooleanConstraint implements Predicate<Value> {
    private static final Logger log = LoggerFactory.getLogger(BooleanConstraint.class);

    private final Boolean requiredValue;

    public BooleanConstraint(String definition)  {
        if ("true".equals(definition)) {
            requiredValue = true;
        }
        else if ("false".equals(definition)) {
            requiredValue = false;
        }
        else {
            requiredValue = null;
            log.warn('\'' + definition + "' is not a valid value constraint format for boolean values");
        }
    }

    @Override
    public boolean apply(Value value) {
        try {
            return value != null && requiredValue != null && value.getBoolean() == requiredValue;
        }
        catch (RepositoryException e) {
            log.warn("Error checking boolean constraint " + this, e);
            return false;
        }
    }

    @Override
    public String toString() {
        return "'" + requiredValue + '\'';
    }
}
