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
package org.apache.jackrabbit.mongomk.api.instruction;

import org.apache.jackrabbit.mongomk.api.instruction.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.CopyNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.MoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.RemoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.SetPropertyInstruction;

/**
 * A <a href="http://en.wikipedia.org/wiki/Visitor_pattern">Visitor</a> to iterate
 * through a list of {@code Instruction}s without the need to use {@code instanceof}
 * on each item.
 */
public interface InstructionVisitor {

    /**
     * Visits a {@code AddNodeInstruction}.
     *
     * @param instruction The instruction.
     */
    void visit(AddNodeInstruction instruction);

    /**
     * Visits a {@code CopyNodeInstruction}.
     *
     * @param instruction The instruction.
     */
    void visit(CopyNodeInstruction instruction);

    /**
     * Visits a {@code MoveNodeInstruction}.
     *
     * @param instruction The instruction.
     */
    void visit(MoveNodeInstruction instruction);

    /**
     * Visits a {@code RemoveNodeInstruction}.
     *
     * @param instruction The instruction.
     */
    void visit(RemoveNodeInstruction instruction);

    /**
     * Visits a {@code SetPropertyInstruction}.
     *
     * @param instruction The instruction.
     */
    void visit(SetPropertyInstruction instruction);
}