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
package org.apache.jackrabbit.oak.plugins.type.constraint;

import javax.jcr.PropertyType;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Constraints {
    private static final Logger log = LoggerFactory.getLogger(Constraints.class);

    private Constraints() {
    }

    public static Predicate<Value> valueConstraint(int type, String constraint) {
        switch (type) {
            case PropertyType.STRING:
                return stringConstraint(constraint);
            case PropertyType.BINARY:
                return binaryConstraint(constraint);
            case PropertyType.LONG:
                return longConstraint(constraint);
            case PropertyType.DOUBLE:
                return doubleConstraint(constraint);
            case PropertyType.DATE:
                return dateConstraint(constraint);
            case PropertyType.BOOLEAN:
                return booleanConstraint(constraint);
            case PropertyType.NAME:
                return nameConstraint(constraint);
            case PropertyType.PATH:
                return pathConstraint(constraint);
            case PropertyType.REFERENCE:
                return referenceConstraint(constraint);
            case PropertyType.WEAKREFERENCE:
                return weakRefConstraint(constraint);
            case PropertyType.URI:
                return uriConstraint(constraint);
            case PropertyType.DECIMAL:
                return decimalConstraint(constraint);
            default:
                String msg = "Invalid property type: " + type;
                log.warn(msg);
                throw new IllegalArgumentException(msg);
        }
    }

    private static Predicate<Value> stringConstraint(String constraint) {
        return new StringConstraint(constraint);
    }

    private static Predicate<Value> binaryConstraint(String constraint) {
        return new BinaryConstraint(constraint);
    }

    private static Predicate<Value> longConstraint(String constraint) {
        return new LongConstraint(constraint);
    }

    private static Predicate<Value> doubleConstraint(String constraint) {
        return new DoubleConstraint(constraint);
    }

    private static Predicate<Value> dateConstraint(String constraint) {
        return new DateConstraint(constraint);
    }

    private static BooleanConstraint booleanConstraint(String constraint) {
        return new BooleanConstraint(constraint);
    }

    private static Predicate<Value> nameConstraint(String constraint) {
        return Predicates.alwaysTrue(); // todo implement nameConstraint
    }

    private static Predicate<Value> pathConstraint(String constraint) {
        return Predicates.alwaysTrue(); // todo implement pathConstraint
    }

    private static Predicate<Value> referenceConstraint(String constraint) {
        return Predicates.alwaysTrue(); // todo implement referenceConstraint
    }

    private static Predicate<Value> weakRefConstraint(String constraint) {
        return Predicates.alwaysTrue(); // todo implement weakRefConstraint
    }

    private static Predicate<Value> uriConstraint(String constraint) {
        return new StringConstraint(constraint);
    }

    private static Predicate<Value> decimalConstraint(String constraint) {
        return new DecimalConstraint(constraint);
    }
}
