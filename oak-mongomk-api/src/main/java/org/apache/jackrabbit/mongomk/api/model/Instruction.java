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
package org.apache.jackrabbit.mongomk.api.model;

/**
 * An {@code Instruction} is an abstraction of a single <a href="http://wiki.apache.org/jackrabbit/Jsop">JSOP</a>
 * operation.
 *
 * <p>
 * Each operation is a concrete subinterface of {@code Instruction} and extending it by the specific properties of the
 * operation. There is no exact 1 : 1 mapping between a {@code JSOP} operation and a subinterface, i.e. in {@code JSOP}
 * there is one add operation for adding nodes and properties whereas there are two specific subinterfaces; one for
 * adding a node and one for adding a property.
 * </p>
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public interface Instruction {

    /**
     * Accepts an {@code InstructionVisitor}.
     *
     * @param visitor The visitor.
     */
    void accept(InstructionVisitor visitor);

    /**
     * Returns the path of this {@code Instruction}.
     *
     * <p>
     * The semantics of this property differ depending on the concrete subinterface.
     * </p>
     *
     * @return The path.
     */
    String getPath();

    /**
     * The add node operation => "+" STRING ":" (OBJECT).
     */
    public interface AddNodeInstruction extends Instruction {
    }

    /**
     * The add property operation => "+" STRING ":" (ATOM | ARRAY)
     */
    public interface AddPropertyInstruction extends Instruction {

        /**
         * Returns the key of the property to add.
         *
         * @return The key.
         */
        String getKey();

        /**
         * Returns the value of the property to add.
         *
         * @return The value.
         */
        Object getValue();
    }

    /**
     * The copy node operation => "*" STRING ":" STRING
     */
    public interface CopyNodeInstruction extends Instruction {

        /**
         * Returns the destination path.
         *
         * @return the destination path.
         */
        String getDestPath();

        /**
         * Returns the source path.
         *
         * @return the source path.
         */
        String getSourcePath();
    }

    /**
     * The move node operation => ">" STRING ":" STRING
     */
    public interface MoveNodeInstruction extends Instruction {

        /**
         * Returns the destination path.
         *
         * @return the destination path.
         */
        String getDestPath();

        /**
         * Returns the source path.
         *
         * @return the source path.
         */
        String getSourcePath();
    }

    /**
     * The remove node operation => "-" STRING
     */
    public interface RemoveNodeInstruction extends Instruction {
    }

    /**
     * The set property operation => "^" STRING ":" ATOM | ARRAY
     */
    public interface SetPropertyInstruction extends Instruction {

        /**
         * Returns the key of the property to set.
         *
         * @return The key.
         */
        String getKey();

        /**
         * Returns the value of the property to set.
         *
         * @return The value.
         */
        Object getValue();
    }
}