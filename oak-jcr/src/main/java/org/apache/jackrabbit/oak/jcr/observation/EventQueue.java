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
package org.apache.jackrabbit.oak.jcr.observation;

import static com.google.common.collect.Lists.newLinkedList;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.EventGenerator;
import org.apache.jackrabbit.oak.plugins.observation.EventHandler;
import org.apache.jackrabbit.oak.plugins.observation.FilteredHandler;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventAggregator;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Queue of JCR Events generated from a given content change
 */
class EventQueue implements EventIterator {

    private final EventGenerator generator;

    private final LinkedList<Event> queue = newLinkedList();

    private long position = 0;

    public EventQueue(
            @Nonnull NamePathMapper mapper, CommitInfo info,
            @Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull Iterable<String> basePaths, @Nonnull EventFilter filter,
            @Nullable EventAggregator aggregator) {
        this.generator = new EventGenerator();
        EventFactory factory = new EventFactory(mapper, info);
        EventHandler handler = new FilteredHandler(
                filter, new QueueingHandler(this, factory, aggregator, before, after));
        for (String path : basePaths) {
            addHandler(before, after, path, handler, generator);
        }
    }

    private static void addHandler(NodeState before, NodeState after, String path,
            EventHandler handler, EventGenerator generator) {
        for (String name : PathUtils.elements(path)) {
            before = before.getChildNode(name);
            after = after.getChildNode(name);
            handler = handler.getChildHandler(name, before, after);
            if (handler == null) {
                return;
            }
        }
        generator.addHandler(before, after, handler);
    }

    /**
     * Called by the {@link QueueingHandler} to add new events to the queue.
     */
    void addEvent(Event event) {
        queue.add(event);
    }

    //-----------------------------------------------------< EventIterator >--

    @Override
    public long getSize() {
        if (generator.isDone()) {
            // no more new events will be generated, so count just those
            // that have already been iterated and those left in the queue
            return position + queue.size();
        } else {
            // the generator is not yet done, so there's no way
            // to know how many events may still be generated
            return -1;
        }
    }

    @Override
    public long getPosition() {
        return position;
    }

    @Override
    public boolean hasNext() {
        while (queue.isEmpty()) {
            if (generator.isDone()) {
                return false;
            } else {
                generator.generate();
            }
        }
        return true;
    }

    @Override
    public void skip(long skipNum) {
        // generate events until all events to skip have been queued
        while (skipNum > queue.size()) {
            // drop all currently queued events as we're skipping them all
            position += queue.size();
            skipNum -= queue.size();
            queue.clear();

            // generate more events if possible, otherwise fail
            if (!generator.isDone()) {
                generator.generate();
            } else {
                throw new NoSuchElementException("Not enough events to skip");
            }
        }

        // the remaining events to skip are guaranteed to all be in the
        // queue, so we can just drop those events and advance the position
        queue.subList(0, (int) skipNum).clear();
        position += skipNum;
    }

    @Override
    public Event nextEvent() {
        if (hasNext()) {
            position++;
            return queue.removeFirst();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Event next() {
        return nextEvent();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
