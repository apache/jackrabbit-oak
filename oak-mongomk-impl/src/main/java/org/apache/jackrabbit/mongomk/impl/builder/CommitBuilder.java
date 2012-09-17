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
package org.apache.jackrabbit.mongomk.impl.builder;

import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.CopyNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.MoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.RemoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.SetPropertyInstruction;
import org.apache.jackrabbit.mongomk.impl.json.DefaultJsopHandler;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.apache.jackrabbit.mongomk.impl.model.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.AddPropertyInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.CommitImpl;
import org.apache.jackrabbit.mongomk.impl.model.CopyNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.MoveNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.RemoveNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.SetPropertyInstructionImpl;

/**
 * A builder to convert a <a href="http://wiki.apache.org/jackrabbit/Jsop">JSOP</a> diff into a {@link Commit}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class CommitBuilder {

    /**
     * Creates and returns the {@link Commit}.
     *
     * @param path The root path of the {@code Commit}.
     * @param diff The {@code JSOP} diff of the {@code Commit}.
     * @param message The message of the {@code Commit}.
     *
     * @return The {@code Commit}.
     * @throws Exception If an error occurred while creating the {@code Commit}.
     */
    public static Commit build(String path, String diff, String message) throws Exception {
        CommitHandler commitHandler = new CommitHandler(new CommitImpl(path, diff, message));
        JsopParser jsopParser = new JsopParser(path, diff, commitHandler);
        jsopParser.parse();
        return commitHandler.getCommit();
    }

    private CommitBuilder() {
        // no instantiation
    }

    /**
     * The {@link DefaultHandler} for the {@code JSOP} diff.
     */
    private static class CommitHandler extends DefaultJsopHandler {
        private final CommitImpl commit;

        CommitHandler(CommitImpl commit) {
            this.commit = commit;
        }

        @Override
        public void nodeAdded(String parentPath, String name) {
            AddNodeInstruction instruction = new AddNodeInstructionImpl(parentPath, name);
            commit.addInstruction(instruction);
        }

        @Override
        public void nodeCopied(String rootPath, String oldPath, String newPath) {
            CopyNodeInstruction instruction = new CopyNodeInstructionImpl(rootPath, oldPath, newPath);
            commit.addInstruction(instruction);
        }

        @Override
        public void nodeMoved(String rootPath, String oldPath, String newPath) {
            MoveNodeInstruction instruction = new MoveNodeInstructionImpl(rootPath, oldPath, newPath);
            commit.addInstruction(instruction);
        }

        @Override
        public void nodeRemoved(String parentPath, String name) {
            RemoveNodeInstruction instruction = new RemoveNodeInstructionImpl(parentPath, name);
            commit.addInstruction(instruction);
        }

        @Override
        public void propertyAdded(String path, String key, Object value) {
            AddPropertyInstruction instruction = new AddPropertyInstructionImpl(path, key, value);
            commit.addInstruction(instruction);
        }

        @Override
        public void propertySet(String path, String key, Object value) {
            SetPropertyInstruction instruction = new SetPropertyInstructionImpl(path, key, value);
            commit.addInstruction(instruction);
        }

        Commit getCommit() {
            return commit;
        }
    }
}
