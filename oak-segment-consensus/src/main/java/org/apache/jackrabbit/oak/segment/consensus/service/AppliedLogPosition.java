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
package org.apache.jackrabbit.oak.segment.consensus.service;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Watermark of the last replicated log entry whose Oak mutation is in the store: the Aeron log position
 * at the end of the entry, the item index inside a batch entry (0 otherwise) and the leadership term.
 *
 * <p>It is written in the same Oak merge as the mutation, under the hidden root node {@value #NODE_NAME},
 * so the store and the watermark can never disagree. Entries at or below it are already in the store.
 */
public final class AppliedLogPosition implements Comparable<AppliedLogPosition> {

    public static final String NODE_NAME = ":consensus";
    static final String POSITION = "appliedLogPosition";
    static final String ITEM = "appliedLogItem";
    static final String TERM = "appliedTerm";

    /** Nothing applied yet (fresh store, or a store written before the watermark existed). */
    public static final AppliedLogPosition NONE = new AppliedLogPosition(0, 0, -1);

    private final long position;
    private final int item;
    private final long term;

    public AppliedLogPosition(long position, int item, long term) {
        this.position = position;
        this.item = item;
        this.term = term;
    }

    public long position() {
        return position;
    }

    public int item() {
        return item;
    }

    public long term() {
        return term;
    }

    public boolean isNone() {
        return position <= 0;
    }

    /** True if the entry item at {@code (logPosition, itemIndex)} is at or below this watermark; NONE covers nothing. */
    public boolean covers(long logPosition, int itemIndex) {
        if (isNone()) {
            return false;
        }
        return logPosition < position || (logPosition == position && itemIndex <= item);
    }

    @NotNull
    public static AppliedLogPosition read(@Nullable NodeState root) {
        if (root == null) {
            return NONE;
        }
        NodeState node = root.getChildNode(NODE_NAME);
        PropertyState position = node.getProperty(POSITION);
        if (position == null) {
            return NONE;
        }
        PropertyState item = node.getProperty(ITEM);
        PropertyState term = node.getProperty(TERM);
        return new AppliedLogPosition(
            position.getValue(Type.LONG),
            item != null ? item.getValue(Type.LONG).intValue() : 0,
            term != null ? term.getValue(Type.LONG) : -1L);
    }

    /** Records this watermark in {@code rootBuilder}; call before merging the mutation it belongs to. */
    public void writeTo(@NotNull NodeBuilder rootBuilder) {
        NodeBuilder node = rootBuilder.child(NODE_NAME);
        node.setProperty(POSITION, position);
        node.setProperty(ITEM, (long) item);
        node.setProperty(TERM, term);
    }

    @Override
    public int compareTo(@NotNull AppliedLogPosition other) {
        int byPosition = Long.compare(position, other.position);
        return byPosition != 0 ? byPosition : Integer.compare(item, other.item);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AppliedLogPosition)) {
            return false;
        }
        AppliedLogPosition other = (AppliedLogPosition) o;
        return position == other.position && item == other.item && term == other.term;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(position) * 31 + item * 7 + Long.hashCode(term);
    }

    @Override
    public String toString() {
        return "position=" + position + " item=" + item + " term=" + term;
    }
}
