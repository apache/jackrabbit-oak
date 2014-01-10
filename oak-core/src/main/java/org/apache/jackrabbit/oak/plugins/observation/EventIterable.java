/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.observation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link EventGenerator} implementation provides a traversable view for
 * events.
 * @param <T> type of the event returned by this iterator
 */
public class EventIterable<T> extends EventGenerator implements Iterable<T> {
    private static final Logger LOG = LoggerFactory.getLogger(EventIterable.class);

    private final NodeState before;
    private final NodeState after;

    private final EventFilter filter;
    private final IterableListener<T> listener;

    private final LinkedList<EventIterable<T>> childEvents = newLinkedList();

    /**
     * Specialisation of {@link Listener} that provides the events reported
     * to it as an iterator.
     *
     * @param <S> type of the events in the iterator
     */
    public interface IterableListener<S> extends Listener, Iterable<S> {
        @Override
        @Nonnull
        IterableListener<S> create(String name, NodeState before, NodeState after);
    }

    /**
     * Create a new instance of a {@code EventIterator} reporting events to the
     * passed {@code listener} after filtering with the passed {@code filter}.
     *
     * @param before  before state
     * @param after   after state
     * @param filter  filter for filtering changes
     * @param listener  listener for listening to the filtered changes
     */
    public EventIterable(@Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull EventFilter filter, @Nonnull IterableListener<T> listener) {
        super(filter, listener);
        this.before = checkNotNull(before);
        this.after = checkNotNull(after);
        this.filter = checkNotNull(filter);
        this.listener = checkNotNull(listener);
    }

    //------------------------------------------------------------< EventGenerator >---

    @Override
    protected EventGenerator createChildGenerator(String name, NodeState before, NodeState after) {
        EventFilter childFilter = filter.create(name, before, after);
        if (childFilter != null) {
            childEvents.add(new EventIterable<T>(
                    before, after,
                    childFilter,
                    listener.create(name, before, after)));
        }
        return null;
    }

    //----------------------------------------------------------< Iterable >--

    @Override
    public Iterator<T> iterator() {
        CommitFailedException e = EditorDiff.process(
                new VisibleEditor(new MoveDetector(this)),
                before, after);
        if (e != null) {
            LOG.error("Error while extracting observation events", e);
        }

        return new Iterator<T>() {

            private Iterator<T> iterator = listener.iterator();

            @Override
            public boolean hasNext() {
                while (!iterator.hasNext()) {
                    if (childEvents.isEmpty()) {
                        return false;
                    } else {
                        iterator = childEvents.removeFirst().iterator();
                    }
                }
                return true;
            }

            @Override
            public T next() {
                if (hasNext()) {
                    return iterator.next();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

}
