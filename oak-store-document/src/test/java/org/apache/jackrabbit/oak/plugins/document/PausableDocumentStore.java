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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type;
import org.apache.jackrabbit.oak.plugins.document.FailingDocumentStore.Fail;

/**
 * Wraps a document store and can be instructed to pause operations.
 */
public class PausableDocumentStore extends DocumentStoreWrapper {

    public interface PauseCallback {

        /**
         * @return the PauseCallback to use going forward - null to stop
         * doing pauses
         */
        PauseCallback handlePause(List<UpdateOp> remainingOps);
    }

    /**
     * small extension of FailingDocumentStore that doesn't throw an exception but
     * pauses (pauseNow instead of failNow).
     */
    static class PausingFailingDocumentStore extends FailingDocumentStore {

        private PausableDocumentStore pds;

        PausingFailingDocumentStore(DocumentStore store) {
            super(store);
        }

        PausingFailingDocumentStore(DocumentStore store, long seed) {
            super(store, seed);
        }

        private void bondWith(PausableDocumentStore pds) {
            this.pds = pds;
        }

        @Override
        void failNow(List<UpdateOp> remainingOps) {
            pds.pauseNow(remainingOps);
        }
    }

    class Pause {

        Fail f = getFailingDocumentStore().fail();

        private Pause() {
            never();
        }

        Pause afterOp() {
            f.afterOp();
            return this;
        }

        Pause beforeOp() {
            f.beforeOp();
            return this;
        }

        Pause after(int numOps) {
            f.after(numOps);
            return this;
        }

        Pause withType(Type type) {
            f.withType(type);
            return this;
        }

        void never() {
            f.never();
        }

        void once() {
            f.once();
        }

        void eternally() {
            f.eternally();
        }

        Pause randomly(double probability) {
            f.randomly(probability);
            return this;
        }

        Pause on(Collection<? extends Document> collectionInclude) {
            f.on(collectionInclude);
            return this;
        }

        Pause on(String idInclude) {
            f.on(idInclude);
            return this;
        }
    }

    PauseCallback pauseCallback = null;

    PausableDocumentStore(DocumentStore store, long seed) {
        super(new PausingFailingDocumentStore(store, seed));
        bond();
    }

    PausableDocumentStore(DocumentStore store) {
        super(new PausingFailingDocumentStore(store));
        bond();
    }

    private void bond() {
        getFailingDocumentStore().bondWith(this);
    }

    private PausingFailingDocumentStore getFailingDocumentStore() {
        return (PausingFailingDocumentStore) store;
    }

    Pause pauseWith(PauseCallback r) {
        assertNotNull(r);
        pauseCallback = r;
        return new Pause();
    }

    void pauseNow(List<UpdateOp> remainingOps) {
        PauseCallback nextCallback = pauseCallback.handlePause(remainingOps);
        if (nextCallback == null) {
            new Pause().never();
        } else if (nextCallback != pauseCallback) {
            pauseWith(nextCallback);
        } // else continue using the same pauseCallback
    }

    public void noDispose() {
        getFailingDocumentStore().noDispose();
    }
}
