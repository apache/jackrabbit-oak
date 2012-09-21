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
package org.apache.jackrabbit.mongomk.impl.model;

import org.apache.jackrabbit.mongomk.api.model.InstructionVisitor;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddPropertyInstruction;

/**
 * Implementation of {@link AddPropertyInstruction}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class AddPropertyInstructionImpl implements AddPropertyInstruction {

    private final String key;
    private final String path;
    private final Object value;

    /**
     * Constructs a new {@code AddPropertyInstructionImpl}.
     *
     * @param path The path.
     * @param key The key.
     * @param value The value.
     */
    public AddPropertyInstructionImpl(String path, String key, Object value) {
        this.path = path;
        this.key = key;
        this.value = value;
    }

    @Override
    public void accept(InstructionVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AddPropertyInstructionImpl [path=");
        builder.append(path);
        builder.append(", key=");
        builder.append(key);
        builder.append(", value=");
        builder.append(value);
        builder.append("]");
        return builder.toString();
    }
}
