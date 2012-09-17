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
package org.apache.jackrabbit.mongomk.impl;

import static junit.framework.Assert.assertEquals;

import org.apache.jackrabbit.mongomk.api.model.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.CopyNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.MoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.RemoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.SetPropertyInstruction;


/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class InstructionAssert {

    public static void assertAddNodeInstruction(AddNodeInstruction instruction, String path) {
        assertEquals(path, instruction.getPath());
    }

    public static void assertAddPropertyInstruction(AddPropertyInstruction instruction, String path, String key,
            Object value) {
        assertEquals(path, instruction.getPath());
        assertEquals(key, instruction.getKey());
        assertEquals(value, instruction.getValue());
    }

    public static void assertCopyNodeInstruction(CopyNodeInstruction instruction, String path, String sourcePath,
            String destPath) {
        assertEquals(path, instruction.getPath());
        assertEquals(sourcePath, instruction.getSourcePath());
        assertEquals(destPath, instruction.getDestPath());
    }

    public static void assertMoveNodeInstruction(MoveNodeInstruction instruction, String parentPath, String oldPath,
            String newPath) {
        assertEquals(parentPath, instruction.getPath());
        assertEquals(oldPath, instruction.getSourcePath());
        assertEquals(newPath, instruction.getDestPath());
    }

    public static void assertRemoveNodeInstruction(RemoveNodeInstruction instruction, String path) {
        assertEquals(path, instruction.getPath());
    }

    public static void assertSetPropertyInstruction(SetPropertyInstruction instruction, String path, String key,
            Object value) {
        assertEquals(path, instruction.getPath());
        assertEquals(key, instruction.getKey());
        assertEquals(value, instruction.getValue());
    }

    private InstructionAssert() {
        // no instantiation
    }
}
