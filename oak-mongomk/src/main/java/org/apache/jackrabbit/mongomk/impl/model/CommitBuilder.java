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

import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.instruction.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.instruction.CopyNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.instruction.MoveNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.instruction.RemoveNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.instruction.SetPropertyInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.json.DefaultJsopHandler;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * A builder to convert a <a href="http://wiki.apache.org/jackrabbit/Jsop">JSOP</a>
 * diff into a {@link Commit}.
 */
public class CommitBuilder {

    /**
     * Creates and returns a {@link Commit} without a base revision id.
     *
     * @param path The root path of the {@code Commit}.
     * @param diff The {@code JSOP} diff of the {@code Commit}.
     * @param message The message of the {@code Commit}.
     *
     * @return The {@code Commit}.
     * @throws Exception If an error occurred while creating the {@code Commit}.
     */
    public static Commit build(String path, String diff, String message)
            throws Exception {
        return CommitBuilder.build(path, diff, null, message);
    }

    /**
     * Creates and returns a {@link Commit}.
     *
     * @param path The root path of the {@code Commit}.
     * @param diff The {@code JSOP} diff of the {@code Commit}.
     * @param revisionId The revision id the commit is based on.
     * @param message The message of the {@code Commit}.
     *
     * @return The {@code Commit}.
     * @throws Exception If an error occurred while creating the {@code Commit}.
     */
    public static Commit build(String path, String diff, String revisionId,
            String message) throws Exception {
        MongoCommit commit = new MongoCommit();
        commit.setBaseRevisionId(MongoUtil.toMongoRepresentation(revisionId));
        commit.setDiff(diff);
        commit.setMessage(message);
        commit.setPath(path);

        JsopParser jsopParser = new JsopParser(path, diff, new CommitHandler(commit));
        jsopParser.parse();

        return commit;
    }

    /**
     * The {@link DefaultJsopHandler} for the {@code JSOP} diff.
     */
    private static class CommitHandler extends DefaultJsopHandler {
        private final MongoCommit commit;

        CommitHandler(MongoCommit commit) {
            this.commit = commit;
        }

        @Override
        public void nodeAdded(String parentPath, String name) {
            commit.addInstruction(new AddNodeInstructionImpl(parentPath, name));
        }

        @Override
        public void nodeCopied(String rootPath, String oldPath, String newPath) {
            commit.addInstruction(new CopyNodeInstructionImpl(rootPath, oldPath, newPath));
        }

        @Override
        public void nodeMoved(String rootPath, String oldPath, String newPath) {
            commit.addInstruction(new MoveNodeInstructionImpl(rootPath, oldPath, newPath));
        }

        @Override
        public void nodeRemoved(String parentPath, String name) {
            commit.addInstruction(new RemoveNodeInstructionImpl(parentPath, name));
        }

        @Override
        public void propertySet(String path, String key, Object value, String rawValue) {
            commit.addInstruction(new SetPropertyInstructionImpl(path,
                    MongoUtil.toMongoPropertyKey(key), value));
        }
    }
}
