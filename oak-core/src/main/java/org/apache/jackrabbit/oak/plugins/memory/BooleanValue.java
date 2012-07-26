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
package org.apache.jackrabbit.oak.plugins.memory;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CoreValue;

abstract class BooleanValue extends MemoryValue {

    public static final CoreValue TRUE = new BooleanValue() {
        @Override
        public boolean getBoolean() {
            return true;
        }
    };

    public static final CoreValue FALSE = new BooleanValue() {
        @Override
        public boolean getBoolean() {
            return false;
        }
    };

    public static CoreValue create(boolean value) {
        if (value) {
            return TRUE;
        } else {
            return FALSE;
        }
    }

    @Override
    public int getType() {
        return PropertyType.BOOLEAN;
    }

    @Override
    public String getString() {
        return Boolean.toString(getBoolean());
    }

}
