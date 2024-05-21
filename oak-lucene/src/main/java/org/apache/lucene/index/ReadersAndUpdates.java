/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.NumericFieldUpdates.UpdatesIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MutableBits;

// Used by IndexWriter to hold open SegmentReaders (for
// searching or merging), plus pending deletes and updates,
// for a given segment
class ReadersAndUpdates {

    // Not final because we replace (clone) when we need to
    // change it and it's been shared:
    public final SegmentCommitInfo info;

    // Tracks how many consumers are using this instance:
    private final AtomicInteger refCount = new AtomicInteger(1);

    private final IndexWriter writer;

    // Set once (null, and then maybe set, and never set again):
    private SegmentReader reader;

    // TODO: it's sometimes wasteful that we hold open two
    // separate SRs (one for merging one for
    // reading)... maybe just use a single SR?  The gains of
    // not loading the terms index (for merging in the
    // non-NRT case) are far less now... and if the app has
    // any deletes it'll open real readers anyway.

    // Set once (null, and then maybe set, and never set again):
    private SegmentReader mergeReader;

    // Holds the current shared (readable and writable)
    // liveDocs.  This is null when there are no deleted
    // docs, and it's copy-on-write (cloned whenever we need
    // to change it but it's been shared to an external NRT
    // reader).
    private Bits liveDocs;

    // How many further deletions we've done against
    // liveDocs vs when we loaded it or last wrote it:
    private int pendingDeleteCount;

    // True if the current liveDocs is referenced by an
    // external NRT reader:
    private boolean liveDocsShared;

    // Indicates whether this segment is currently being merged. While a segment
    // is merging, all field updates are also registered in the
    // mergingNumericUpdates map. Also, calls to writeFieldUpdates merge the
    // updates with mergingNumericUpdates.
    // That way, when the segment is done merging, IndexWriter can apply the
    // updates on the merged segment too.
    private boolean isMerging = false;

    private final Map<String, NumericFieldUpdates> mergingNumericUpdates = new HashMap<String, NumericFieldUpdates>();

    public ReadersAndUpdates(IndexWriter writer, SegmentCommitInfo info) {
        this.info = info;
        this.writer = writer;
        liveDocsShared = true;
    }

    public void incRef() {
        final int rc = refCount.incrementAndGet();
        assert rc > 1;
    }

    public void decRef() {
        final int rc = refCount.decrementAndGet();
        assert rc >= 0;
    }

    public int refCount() {
        final int rc = refCount.get();
        assert rc >= 0;
        return rc;
    }

    public synchronized int getPendingDeleteCount() {
        return pendingDeleteCount;
    }

    // Call only from assert!
    public synchronized boolean verifyDocCounts() {
        int count;
        if (liveDocs != null) {
            count = 0;
            for (int docID = 0; docID < info.info.getDocCount(); docID++) {
                if (liveDocs.get(docID)) {
                    count++;
                }
            }
        } else {
            count = info.info.getDocCount();
        }

        assert info.info.getDocCount() - info.getDelCount() - pendingDeleteCount == count :
            "info.docCount=" + info.info.getDocCount() + " info.getDelCount()=" + info.getDelCount()
                + " pendingDeleteCount=" + pendingDeleteCount + " count=" + count;
        return true;
    }

    /**
     * Returns a {@link SegmentReader}.
     */
    public SegmentReader getReader(IOContext context) throws IOException {
        if (reader == null) {
            // We steal returned ref:
            reader = new SegmentReader(info, writer.getConfig().getReaderTermsIndexDivisor(),
                context);
            if (liveDocs == null) {
                liveDocs = reader.getLiveDocs();
            }
        }

        // Ref for caller
        reader.incRef();
        return reader;
    }

    // Get reader for merging (does not load the terms
    // index):
    public synchronized SegmentReader getMergeReader(IOContext context) throws IOException {
        //System.out.println("  livedocs=" + rld.liveDocs);

        if (mergeReader == null) {

            if (reader != null) {
                // Just use the already opened non-merge reader
                // for merging.  In the NRT case this saves us
                // pointless double-open:
                //System.out.println("PROMOTE non-merge reader seg=" + rld.info);
                // Ref for us:
                reader.incRef();
                mergeReader = reader;
                //System.out.println(Thread.currentThread().getName() + ": getMergeReader share seg=" + info.name);
            } else {
                //System.out.println(Thread.currentThread().getName() + ": getMergeReader seg=" + info.name);
                // We steal returned ref:
                mergeReader = new SegmentReader(info, -1, context);
                if (liveDocs == null) {
                    liveDocs = mergeReader.getLiveDocs();
                }
            }
        }

        // Ref for caller
        mergeReader.incRef();
        return mergeReader;
    }

    public synchronized void release(SegmentReader sr) throws IOException {
        assert info == sr.getSegmentInfo();
        sr.decRef();
    }

    public synchronized boolean delete(int docID) {
        assert liveDocs != null;
        assert Thread.holdsLock(writer);
        assert
            docID >= 0 && docID < liveDocs.length() :
            "out of bounds: docid=" + docID + " liveDocsLength=" + liveDocs.length() + " seg="
                + info.info.name + " docCount=" + info.info.getDocCount();
        assert !liveDocsShared;
        final boolean didDelete = liveDocs.get(docID);
        if (didDelete) {
            ((MutableBits) liveDocs).clear(docID);
            pendingDeleteCount++;
            //System.out.println("  new del seg=" + info + " docID=" + docID + " pendingDelCount=" + pendingDeleteCount + " totDelCount=" + (info.docCount-liveDocs.count()));
        }
        return didDelete;
    }

    // NOTE: removes callers ref
    public synchronized void dropReaders() throws IOException {
        // TODO: can we somehow use IOUtils here...?  problem is
        // we are calling .decRef not .close)...
        try {
            if (reader != null) {
                //System.out.println("  pool.drop info=" + info + " rc=" + reader.getRefCount());
                try {
                    reader.decRef();
                } finally {
                    reader = null;
                }
            }
        } finally {
            if (mergeReader != null) {
                //System.out.println("  pool.drop info=" + info + " merge rc=" + mergeReader.getRefCount());
                try {
                    mergeReader.decRef();
                } finally {
                    mergeReader = null;
                }
            }
        }

        decRef();
    }

    /**
     * Returns a ref to a clone. NOTE: you should decRef() the reader when you're dont (ie do not
     * call close()).
     */
    public synchronized SegmentReader getReadOnlyClone(IOContext context) throws IOException {
        if (reader == null) {
            getReader(context).decRef();
            assert reader != null;
        }
        liveDocsShared = true;
        if (liveDocs != null) {
            return new SegmentReader(reader.getSegmentInfo(), reader, liveDocs,
                info.info.getDocCount() - info.getDelCount() - pendingDeleteCount);
        } else {
            assert reader.getLiveDocs() == liveDocs;
            reader.incRef();
            return reader;
        }
    }

    public synchronized void initWritableLiveDocs() throws IOException {
        assert Thread.holdsLock(writer);
        assert info.info.getDocCount() > 0;
        //System.out.println("initWritableLivedocs seg=" + info + " liveDocs=" + liveDocs + " shared=" + shared);
        if (liveDocsShared) {
            // Copy on write: this means we've cloned a
            // SegmentReader sharing the current liveDocs
            // instance; must now make a private clone so we can
            // change it:
            LiveDocsFormat liveDocsFormat = info.info.getCodec().liveDocsFormat();
            if (liveDocs == null) {
                //System.out.println("create BV seg=" + info);
                liveDocs = liveDocsFormat.newLiveDocs(info.info.getDocCount());
            } else {
                liveDocs = liveDocsFormat.newLiveDocs(liveDocs);
            }
            liveDocsShared = false;
        }
    }

    public synchronized Bits getLiveDocs() {
        assert Thread.holdsLock(writer);
        return liveDocs;
    }

    public synchronized Bits getReadOnlyLiveDocs() {
        //System.out.println("getROLiveDocs seg=" + info);
        assert Thread.holdsLock(writer);
        liveDocsShared = true;
        //if (liveDocs != null) {
        //System.out.println("  liveCount=" + liveDocs.count());
        //}
        return liveDocs;
    }

    public synchronized void dropChanges() {
        // Discard (don't save) changes when we are dropping
        // the reader; this is used only on the sub-readers
        // after a successful merge.  If deletes had
        // accumulated on those sub-readers while the merge
        // is running, by now we have carried forward those
        // deletes onto the newly merged segment, so we can
        // discard them on the sub-readers:
        pendingDeleteCount = 0;
        dropMergingUpdates();
    }

    // Commit live docs (writes new _X_N.del files) and field updates (writes new
    // _X_N updates files) to the directory; returns true if it wrote any file
    // and false if there were no new deletes or updates to write:
    // TODO (DVU_RENAME) to writeDeletesAndUpdates
    public synchronized boolean writeLiveDocs(Directory dir) throws IOException {
        assert Thread.holdsLock(writer);
        //System.out.println("rld.writeLiveDocs seg=" + info + " pendingDelCount=" + pendingDeleteCount + " numericUpdates=" + numericUpdates);
        if (pendingDeleteCount == 0) {
            return false;
        }

        // We have new deletes
        assert liveDocs.length() == info.info.getDocCount();

        // Do this so we can delete any created files on
        // exception; this saves all codecs from having to do
        // it:
        TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

        // We can write directly to the actual name (vs to a
        // .tmp & renaming it) because the file is not live
        // until segments file is written:
        boolean success = false;
        try {
            Codec codec = info.info.getCodec();
            codec.liveDocsFormat()
                 .writeLiveDocs((MutableBits) liveDocs, trackingDir, info, pendingDeleteCount,
                     IOContext.DEFAULT);
            success = true;
        } finally {
            if (!success) {
                // Advance only the nextWriteDelGen so that a 2nd
                // attempt to write will write to a new file
                info.advanceNextWriteDelGen();

                // Delete any partially created file(s):
                for (String fileName : trackingDir.getCreatedFiles()) {
                    try {
                        dir.deleteFile(fileName);
                    } catch (Throwable t) {
                        // Ignore so we throw only the first exc
                    }
                }
            }
        }

        // If we hit an exc in the line above (eg disk full)
        // then info's delGen remains pointing to the previous
        // (successfully written) del docs:
        info.advanceDelGen();
        info.setDelCount(info.getDelCount() + pendingDeleteCount);
        pendingDeleteCount = 0;

        return true;
    }

    // Writes field updates (new _X_N updates files) to the directory
    public synchronized void writeFieldUpdates(Directory dir,
        Map<String, NumericFieldUpdates> numericFieldUpdates) throws IOException {
        assert Thread.holdsLock(writer);
        //System.out.println("rld.writeFieldUpdates: seg=" + info + " numericFieldUpdates=" + numericFieldUpdates);

        assert numericFieldUpdates != null && !numericFieldUpdates.isEmpty();

        // Do this so we can delete any created files on
        // exception; this saves all codecs from having to do
        // it:
        TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

        FieldInfos fieldInfos = null;
        boolean success = false;
        try {
            final Codec codec = info.info.getCodec();

            // reader could be null e.g. for a just merged segment (from
            // IndexWriter.commitMergedDeletes).
            final SegmentReader reader = this.reader == null ? new SegmentReader(info,
                writer.getConfig().getReaderTermsIndexDivisor(), IOContext.READONCE) : this.reader;
            try {
                // clone FieldInfos so that we can update their dvGen separately from
                // the reader's infos and write them to a new fieldInfos_gen file
                FieldInfos.Builder builder = new FieldInfos.Builder(writer.globalFieldNumberMap);
                // cannot use builder.add(reader.getFieldInfos()) because it does not
                // clone FI.attributes as well FI.dvGen
                for (FieldInfo fi : reader.getFieldInfos()) {
                    FieldInfo clone = builder.add(fi);
                    // copy the stuff FieldInfos.Builder doesn't copy
                    if (fi.attributes() != null) {
                        for (Entry<String, String> e : fi.attributes().entrySet()) {
                            clone.putAttribute(e.getKey(), e.getValue());
                        }
                    }
                    clone.setDocValuesGen(fi.getDocValuesGen());
                }
                // create new fields or update existing ones to have NumericDV type
                for (String f : numericFieldUpdates.keySet()) {
                    builder.addOrUpdate(f, NumericDocValuesField.TYPE);
                }

                fieldInfos = builder.finish();
                final long nextFieldInfosGen = info.getNextFieldInfosGen();
                final String segmentSuffix = Long.toString(nextFieldInfosGen, Character.MAX_RADIX);
                final SegmentWriteState state = new SegmentWriteState(null, trackingDir, info.info,
                    fieldInfos, writer.getConfig().getTermIndexInterval(), null, IOContext.DEFAULT,
                    segmentSuffix);
                final DocValuesFormat docValuesFormat = codec.docValuesFormat();
                final DocValuesConsumer fieldsConsumer = docValuesFormat.fieldsConsumer(state);
                boolean fieldsConsumerSuccess = false;
                try {
//          System.out.println("[" + Thread.currentThread().getName() + "] RLD.writeLiveDocs: applying updates; seg=" + info + " updates=" + numericUpdates);
                    for (Entry<String, NumericFieldUpdates> e : numericFieldUpdates.entrySet()) {
                        final String field = e.getKey();
                        final NumericFieldUpdates fieldUpdates = e.getValue();
                        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
                        assert fieldInfo != null;

                        fieldInfo.setDocValuesGen(nextFieldInfosGen);
                        // write the numeric updates to a new gen'd docvalues file
                        fieldsConsumer.addNumericField(fieldInfo, new Iterable<Number>() {
                            final NumericDocValues currentValues = reader.getNumericDocValues(
                                field);
                            final Bits docsWithField = reader.getDocsWithField(field);
                            final int maxDoc = reader.maxDoc();
                            final UpdatesIterator updatesIter = fieldUpdates.getUpdates();

                            @Override
                            public Iterator<Number> iterator() {
                                updatesIter.reset();
                                return new Iterator<Number>() {

                                    int curDoc = -1;
                                    int updateDoc = updatesIter.nextDoc();

                                    @Override
                                    public boolean hasNext() {
                                        return curDoc < maxDoc - 1;
                                    }

                                    @Override
                                    public Number next() {
                                        if (++curDoc >= maxDoc) {
                                            throw new NoSuchElementException(
                                                "no more documents to return values for");
                                        }
                                        if (curDoc
                                            == updateDoc) { // this document has an updated value
                                            Long value = updatesIter.value(); // either null (unset value) or updated value
                                            updateDoc = updatesIter.nextDoc(); // prepare for next round
                                            return value;
                                        } else {
                                            // no update for this document
                                            assert curDoc < updateDoc;
                                            if (currentValues != null && docsWithField.get(
                                                curDoc)) {
                                                // only read the current value if the document had a value before
                                                return currentValues.get(curDoc);
                                            } else {
                                                return null;
                                            }
                                        }
                                    }

                                    @Override
                                    public void remove() {
                                        throw new UnsupportedOperationException(
                                            "this iterator does not support removing elements");
                                    }
                                };
                            }
                        });
                    }

                    codec.fieldInfosFormat().getFieldInfosWriter()
                         .write(trackingDir, info.info.name, segmentSuffix, fieldInfos,
                             IOContext.DEFAULT);
                    fieldsConsumerSuccess = true;
                } finally {
                    if (fieldsConsumerSuccess) {
                        fieldsConsumer.close();
                    } else {
                        IOUtils.closeWhileHandlingException(fieldsConsumer);
                    }
                }
            } finally {
                if (reader != this.reader) {
//          System.out.println("[" + Thread.currentThread().getName() + "] RLD.writeLiveDocs: closeReader " + reader);
                    reader.close();
                }
            }

            success = true;
        } finally {
            if (!success) {
                // Advance only the nextWriteDocValuesGen so that a 2nd
                // attempt to write will write to a new file
                info.advanceNextWriteFieldInfosGen();

                // Delete any partially created file(s):
                for (String fileName : trackingDir.getCreatedFiles()) {
                    try {
                        dir.deleteFile(fileName);
                    } catch (Throwable t) {
                        // Ignore so we throw only the first exc
                    }
                }
            }
        }

        info.advanceFieldInfosGen();
        // copy all the updates to mergingUpdates, so they can later be applied to the merged segment
        if (isMerging) {
            for (Entry<String, NumericFieldUpdates> e : numericFieldUpdates.entrySet()) {
                NumericFieldUpdates fieldUpdates = mergingNumericUpdates.get(e.getKey());
                if (fieldUpdates == null) {
                    mergingNumericUpdates.put(e.getKey(), e.getValue());
                } else {
                    fieldUpdates.merge(e.getValue());
                }
            }
        }

        // create a new map, keeping only the gens that are in use
        Map<Long, Set<String>> genUpdatesFiles = info.getUpdatesFiles();
        Map<Long, Set<String>> newGenUpdatesFiles = new HashMap<Long, Set<String>>();
        final long fieldInfosGen = info.getFieldInfosGen();
        for (FieldInfo fi : fieldInfos) {
            long dvGen = fi.getDocValuesGen();
            if (dvGen != -1 && !newGenUpdatesFiles.containsKey(dvGen)) {
                if (dvGen == fieldInfosGen) {
                    newGenUpdatesFiles.put(fieldInfosGen, trackingDir.getCreatedFiles());
                } else {
                    newGenUpdatesFiles.put(dvGen, genUpdatesFiles.get(dvGen));
                }
            }
        }

        info.setGenUpdatesFiles(newGenUpdatesFiles);

        // wrote new files, should checkpoint()
        writer.checkpoint();

        // if there is a reader open, reopen it to reflect the updates
        if (reader != null) {
            SegmentReader newReader = new SegmentReader(info, reader, liveDocs,
                info.info.getDocCount() - info.getDelCount() - pendingDeleteCount);
            boolean reopened = false;
            try {
                reader.decRef();
                reader = newReader;
                reopened = true;
            } finally {
                if (!reopened) {
                    newReader.decRef();
                }
            }
        }
    }

    /**
     * Returns a reader for merge. This method applies field updates if there are any and marks that
     * this segment is currently merging.
     */
    synchronized SegmentReader getReaderForMerge(IOContext context) throws IOException {
        assert Thread.holdsLock(writer);
        // must execute these two statements as atomic operation, otherwise we
        // could lose updates if e.g. another thread calls writeFieldUpdates in
        // between, or the updates are applied to the obtained reader, but then
        // re-applied in IW.commitMergedDeletes (unnecessary work and potential
        // bugs).
        isMerging = true;
        return getReader(context);
    }

    /**
     * Drops all merging updates. Called from IndexWriter after this segment finished merging
     * (whether successfully or not).
     */
    public synchronized void dropMergingUpdates() {
        mergingNumericUpdates.clear();
        isMerging = false;
    }

    /**
     * Returns updates that came in while this segment was merging.
     */
    public synchronized Map<String, NumericFieldUpdates> getMergingFieldUpdates() {
        return mergingNumericUpdates;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReadersAndLiveDocs(seg=").append(info);
        sb.append(" pendingDeleteCount=").append(pendingDeleteCount);
        sb.append(" liveDocsShared=").append(liveDocsShared);
        return sb.toString();
    }

}
