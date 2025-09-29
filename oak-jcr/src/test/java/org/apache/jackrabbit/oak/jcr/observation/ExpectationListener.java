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

import static java.util.Collections.synchronizedList;

import static java.util.Collections.synchronizedSet;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import org.apache.jackrabbit.guava.common.util.concurrent.ForwardingListenableFuture;
import org.apache.jackrabbit.guava.common.util.concurrent.Futures;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.apache.jackrabbit.guava.common.util.concurrent.SettableFuture;

import static org.apache.jackrabbit.oak.jcr.observation.EventFactory.AFTERVALUE;
import static org.apache.jackrabbit.oak.jcr.observation.EventFactory.BEFOREVALUE;


public class ExpectationListener implements EventListener {
    private final Set<Expectation> expected = synchronizedSet(new CopyOnWriteArraySet<>());
    private final Set<Expectation> optional = synchronizedSet(new CopyOnWriteArraySet<>());
    private final List<Event> unexpected = synchronizedList(new CopyOnWriteArrayList<>());

    private volatile Exception failed;

    public Expectation expect(Expectation expectation) {
        if (failed != null) {
            expectation.fail(failed);
        }
        expected.add(expectation);
        return expectation;
    }

    public Expectation optional(Expectation expectation) {
        if (failed != null) {
            expectation.fail(failed);
        }
        optional.add(expectation);
        return expectation;
    }

    public Future<Event> expect(final String path, final int type) {
        return expect(new Expectation("path = " + path + ", type = " + type) {
            @Override
            public boolean onEvent(Event event) throws RepositoryException {
                return type == event.getType() && Objects.equals(path, event.getPath());
            }
        });
    }

    public Future<Event> expect(final String path, final String identifier, final int type) {
        return expect(new Expectation("path = " + path + ", identifier = " + identifier + ", type = " + type) {
            @Override
            public boolean onEvent(Event event) throws RepositoryException {
                return type == event.getType() && Objects.equals(path, event.getPath()) && Objects.equals(identifier, event.getIdentifier());
            }
        });
    }

    public Node expectAdd(Node node) throws RepositoryException {
        expect(node.getPath(), NODE_ADDED);
        expect(node.getPath() + "/jcr:primaryType", PROPERTY_ADDED);
        return node;
    }

    public Node expectRemove(Node node) throws RepositoryException {
        expect(node.getPath(), NODE_REMOVED);
        expect(node.getPath() + "/jcr:primaryType", PROPERTY_REMOVED);
        return node;
    }

    public Property expectAdd(Property property) throws RepositoryException {
        expect(property.getPath(), PROPERTY_ADDED);
        return property;
    }

    public Property expectRemove(Property property) throws RepositoryException {
        expect(property.getPath(), PROPERTY_REMOVED);
        return property;
    }

    public Property expectChange(Property property) throws RepositoryException {
        expect(property.getPath(), PROPERTY_CHANGED);
        return property;
    }

    public void expectMove(final String src, final String dst) {
        expect(new Expectation('>' + src + ':' + dst){
            @Override
            public boolean onEvent(Event event) throws Exception {
                return event.getType() == NODE_MOVED &&
                        Objects.equals(dst, event.getPath()) &&
                        Objects.equals(src, event.getInfo().get("srcAbsPath")) &&
                        Objects.equals(dst, event.getInfo().get("destAbsPath"));
            }
        });
    }

    public void expectValue(final Value before, final Value after) {
        expect(new Expectation("Before value " + before + " after value " + after) {
            @Override
            public boolean onEvent(Event event) throws Exception {
                return Objects.equals(before, event.getInfo().get(BEFOREVALUE)) &&
                        Objects.equals(after, event.getInfo().get(AFTERVALUE));
            }
        });
    }

    public void expectValues(final Value[] before, final Value[] after) {
        expect(new Expectation("Before valuse " + before + " after values " + after) {
            @Override
            public boolean onEvent(Event event) throws Exception {
                return Arrays.equals(before, (Object[])event.getInfo().get(BEFOREVALUE)) &&
                        Arrays.equals(after, (Object[]) event.getInfo().get(AFTERVALUE));
            }
        });
    }

    public void expectEvent(String name, Function<Event,Boolean> validation) {
        expect(new Expectation(name) {
            @Override
            public boolean onEvent(Event event) throws Exception {
                return validation.apply(event);
            }
        });
    }

    public Future<Event> expectBeforeValue(final String path, final int type, final String beforeValue) {
        return expect(new Expectation("path = " + path + ", type = " + type + ", beforeValue = " + beforeValue) {
            @Override
            public boolean onEvent(Event event) throws RepositoryException {
                return type == event.getType() 
                        && Objects.equals(path, event.getPath()) 
                        && event.getInfo().containsKey(BEFOREVALUE) 
                        && beforeValue.equals(((Value)event.getInfo().get(BEFOREVALUE)).getString());
            }
        });
    }

    public List<Expectation> getMissing(int time, TimeUnit timeUnit)
            throws ExecutionException, InterruptedException {
        List<Expectation> missing = new ArrayList<>();
        try {
            Futures.allAsList(expected).get(time, timeUnit);
        }
        catch (TimeoutException e) {
            for (Expectation exp : expected) {
                if (!exp.isDone()) {
                    missing.add(exp);
                }
            }
        }
        return missing;
    }

    public List<Event> getUnexpected() {
        return new ArrayList<>(unexpected);
    }

    @Override
    public void onEvent(EventIterator events) {
        try {
            while (events.hasNext() && failed == null) {
                Event event = events.nextEvent();
                boolean found = false;
                for (Expectation exp : expected) {
                    if (exp.isEnabled() && !exp.isComplete() && exp.onEvent(event)) {
                        found = true;
                        exp.complete(event);
                    }
                }
                for (Expectation opt : optional) {
                    if (opt.isEnabled() && !opt.isComplete() && opt.onEvent(event)) {
                        found = true;
                        opt.complete(event);
                    }
                }
                if (!found) {
                    unexpected.add(event);
                }

            }
        } catch (Exception e) {
            for (Expectation exp : expected) {
                exp.fail(e);
            }
            failed = e;
        }
    }

    public static class Expectation extends ForwardingListenableFuture<Event> {
        private final SettableFuture<Event> future = SettableFuture.create();
        private final String name;

        private volatile boolean enabled = true;

        Expectation(String name, boolean enabled) {
            this.name = name;
            this.enabled = enabled;
        }

        Expectation(String name) {
            this(name, true);
        }

        @Override
        protected ListenableFuture<Event> delegate() {
            return future;
        }

        public void enable(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void complete(Event event) {
            future.set(event);
        }

        public boolean isComplete() {
            return future.isDone();
        }

        public void fail(Exception e) {
            future.setException(e);
        }

        public boolean wait(long timeout, TimeUnit unit) {
            try {
                future.get(timeout, unit);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }

        /**
         * Handle the event
         * @param event the event
         * @return if the event should be handled
         * @throws Exception
         */
        public boolean onEvent(Event event) throws Exception {
            return true;
        }

        @Override
        public String toString() {
            return name;
        }
    }


}
