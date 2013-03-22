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

import javax.jcr.PropertyType;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Constraints {
    private static final Logger log = LoggerFactory.getLogger(Constraints.class);

    private Constraints() {
    }

    public static Predicate<Value> valueConstraint(int type, String constraint) {
        switch (type) {
            case PropertyType.STRING:
                return new StringConstraint(constraint);
            case PropertyType.BINARY:
                return new BinaryConstraint(constraint);
            case PropertyType.LONG:
                return new LongConstraint(constraint);
            case PropertyType.DOUBLE:
                return new DoubleConstraint(constraint);
            case PropertyType.DATE:
                return new DateConstraint(constraint);
            case PropertyType.BOOLEAN:
                return new BooleanConstraint(constraint);
            case PropertyType.NAME:
                return new NameConstraint(constraint);
            case PropertyType.PATH:
                return new PathConstraint(constraint);
            case PropertyType.REFERENCE:
                return new ReferenceConstraint(constraint);
            case PropertyType.WEAKREFERENCE:
                return new ReferenceConstraint(constraint);
            case PropertyType.URI:
                return new StringConstraint(constraint);
            case PropertyType.DECIMAL:
                return new DecimalConstraint(constraint);
            default:
                String msg = "Invalid property type: " + type;
                log.warn(msg);
                throw new IllegalArgumentException(msg);
        }
    }

}
