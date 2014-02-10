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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

/**
 * Test for OAK-695.
 */
public class TypeEditorTest {

    @Test
    public void ignoreHidden() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();
        builder.child(":hidden");
        NodeState after = builder.getNodeState();
        hook.processCommit(before, after, CommitInfo.EMPTY);

        before = after;
        builder.child(":hidden").setProperty("prop", "value");
        after = builder.getNodeState();
        hook.processCommit(before, after, CommitInfo.EMPTY);

        before = after;
        builder.getChildNode(":hidden").remove();
        after = builder.getNodeState();
        hook.processCommit(before, after, CommitInfo.EMPTY);
    }

    @Test
    public void removeNonMandatoryProperty() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryProperty("mandatory")).andReturn(false);

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.propertyDeleted(PropertyStates.createProperty("mandatory", ""));
    }

    @Test(expected = CommitFailedException.class)
    public void removeMandatoryProperty() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryProperty("mandatory")).andReturn(true);
        expect(effective.constraintViolation(
                22, "/", "Mandatory property mandatory can not be removed"))
                .andReturn(new CommitFailedException("", 0, ""));

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.propertyDeleted(PropertyStates.createProperty("mandatory", ""));
    }

    @Test
    public void removeNonMandatoryChildNode() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryChildNode("mandatory")).andReturn(false);

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.childNodeDeleted("mandatory", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void removeMandatoryChildNode() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryChildNode("mandatory")).andReturn(true);
        expect(effective.constraintViolation(
                26, "/", "Mandatory child node mandatory can not be removed"))
                .andReturn(new CommitFailedException("", 0, ""));

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.childNodeDeleted("mandatory", EMPTY_NODE);
    }

}
