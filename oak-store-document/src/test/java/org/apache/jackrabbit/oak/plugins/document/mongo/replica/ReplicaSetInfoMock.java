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
package org.apache.jackrabbit.oak.plugins.document.mongo.replica;

import com.google.common.base.Function;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.stats.Clock;
import org.bson.BasicBSONObject;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.transformValues;
import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicaSetInfoMock extends ReplicaSetInfo {

    private final Clock clock;

    private Clock mongoClock;

    private ReplicaSetMock replicationSet;

    public static ReplicaSetInfoMock create(Clock clock) {
        DB db = mock(DB.class);
        when(db.getName()).thenReturn("oak-db");
        when(db.getSisterDB(Mockito.anyString())).thenReturn(db);
        return new ReplicaSetInfoMock(clock, db);
    }

    private ReplicaSetInfoMock(Clock clock, DB db) {
        super(clock, db, null, 0, Long.MAX_VALUE, null);

        this.clock = clock;
        this.mongoClock = clock;
        this.hiddenMembers = emptyList();
    }

    public void setMongoClock(Clock mongoClock) {
        this.mongoClock = mongoClock;
    }

    public RevisionBuilder addInstance(MemberState state, String name) {
        if (replicationSet == null) {
            replicationSet = new ReplicaSetMock();
        }
        return replicationSet.addInstance(state, name);
    }

    public void updateRevisions() {
        updateReplicaStatus();
        for (ReplicaSetInfoListener listener : listeners) {
            listener.gotRootRevisions(rootRevisions);
        }
    }

    @Override
    protected BasicDBObject getReplicaStatus() {
        BasicDBObject obj = new BasicDBObject();
        obj.put("date", mongoClock.getDate());
        obj.put("members", replicationSet.members);
        return obj;
    }

    @Override
    protected Map<String, Timestamped<RevisionVector>> getRootRevisions(Iterable<String> hosts) {
        return transformValues(replicationSet.memberRevisions,
                new Function<RevisionBuilder, Timestamped<RevisionVector>>() {
                    @Override
                    public Timestamped<RevisionVector> apply(RevisionBuilder input) {
                        return new Timestamped<RevisionVector>(input.revs, clock.getTime());
                    }
                });
    }

    private class ReplicaSetMock {

        private List<BasicBSONObject> members = new ArrayList<BasicBSONObject>();

        private Map<String, RevisionBuilder> memberRevisions = new HashMap<String, RevisionBuilder>();

        private RevisionBuilder addInstance(MemberState state, String name) {
            BasicBSONObject member = new BasicBSONObject();
            member.put("stateStr", state.name());
            member.put("name", name);
            members.add(member);

            RevisionBuilder builder = new RevisionBuilder();
            memberRevisions.put(name, builder);
            return builder;
        }
    }

    public static class RevisionBuilder {

        private RevisionVector revs = new RevisionVector();

        public RevisionBuilder addRevisions(long... timestamps) {
            for (int i = 0; i < timestamps.length; i++) {
                addRevision(timestamps[i], 0, i, false);
            }
            return this;
        }

        public RevisionBuilder addRevision(long timestamp, int counter, int clusterId, boolean branch) {
            Revision rev = new Revision(timestamp, counter, clusterId, branch);
            revs = revs.update(rev);
            return this;
        }

        public RevisionBuilder set(RevisionVector revs) {
            this.revs = revs;
            return this;
        }

        public RevisionVector build() {
            return revs;
        }
    }

}
