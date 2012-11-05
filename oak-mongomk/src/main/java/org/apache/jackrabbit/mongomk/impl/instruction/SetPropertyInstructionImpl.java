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
package org.apache.jackrabbit.mongomk.impl.instruction;

import org.apache.jackrabbit.mongomk.api.instruction.InstructionVisitor;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.SetPropertyInstruction;

/**
 * Implementation for the set property operation => "^" STRING ":" ATOM | ARRAY
 */
public class SetPropertyInstructionImpl extends BaseInstruction implements SetPropertyInstruction {

    private final String key;
    private final Object value;

    /**
     * Constructs a new {@code SetPropertyInstruction}.
     *
     * @param path The path.
     * @param key The key.
     * @param value The value.
     */
    public SetPropertyInstructionImpl(String path, String key, Object value) {
        super(path);
        this.key = key;
        this.value = value;
    }

    @Override
    public void accept(InstructionVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns the name of the property.
     *
     * @return The name of the property.
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the value of the property.
     *
     * @return The value of the property.
     */
    public Object getValue() {
        return value;
    }
}