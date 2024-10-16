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
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.MutableBits;
import org.apache.lucene.util.RamUsageEstimator;

class DocumentsWriterPerThread {

  /**
   * The IndexingChain must define the {@link #getChain(DocumentsWriterPerThread)} method
   * which returns the DocConsumer that the DocumentsWriter calls to process the
   * documents.
   */
  abstract static class IndexingChain {
    abstract DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread);
  }
  

  static final IndexingChain defaultIndexingChain = new IndexingChain() {

    @Override
    DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread) {
      /*
      This is the current indexing chain:

      DocConsumer / DocConsumerPerThread
        --> code: DocFieldProcessor
          --> DocFieldConsumer / DocFieldConsumerPerField
            --> code: DocFieldConsumers / DocFieldConsumersPerField
              --> code: DocInverter / DocInverterPerField
                --> InvertedDocConsumer / InvertedDocConsumerPerField
                  --> code: TermsHash / TermsHashPerField
                    --> TermsHashConsumer / TermsHashConsumerPerField
                      --> code: FreqProxTermsWriter / FreqProxTermsWriterPerField
                      --> code: TermVectorsTermsWriter / TermVectorsTermsWriterPerField
                --> InvertedDocEndConsumer / InvertedDocConsumerPerField
                  --> code: NormsConsumer / NormsConsumerPerField
          --> StoredFieldsConsumer
            --> TwoStoredFieldConsumers
              -> code: StoredFieldsProcessor
              -> code: DocValuesProcessor
    */

    // Build up indexing chain:

      final TermsHashConsumer termVectorsWriter = new TermVectorsConsumer(documentsWriterPerThread);
      final TermsHashConsumer freqProxWriter = new FreqProxTermsWriter();

      final InvertedDocConsumer termsHash = new TermsHash(documentsWriterPerThread, freqProxWriter, true,
                                                          new TermsHash(documentsWriterPerThread, termVectorsWriter, false, null));
      final NormsConsumer normsWriter = new NormsConsumer();
      final DocInverter docInverter = new DocInverter(documentsWriterPerThread.docState, termsHash, normsWriter);
      final StoredFieldsConsumer storedFields = new TwoStoredFieldsConsumers(
                                                      new StoredFieldsProcessor(documentsWriterPerThread),
                                                      new DocValuesProcessor(documentsWriterPerThread.bytesUsed));
      return new DocFieldProcessor(documentsWriterPerThread, docInverter, storedFields);
    }
  };

  static class DocState {
    final DocumentsWriterPerThread docWriter;
    Analyzer analyzer;
    InfoStream infoStream;
    Similarity similarity;
    int docID;
    Iterable<? extends IndexableField> doc;
    String maxTermPrefix;

    DocState(DocumentsWriterPerThread docWriter, InfoStream infoStream) {
      this.docWriter = docWriter;
      this.infoStream = infoStream;
    }

    // Only called by asserts
    public boolean testPoint(String name) {
      return docWriter.testPoint(name);
    }

    public void clear() {
      // don't hold onto doc nor analyzer, in case it is
      // largish:
      doc = null;
      analyzer = null;
    }
  }

  static class FlushedSegment {
    final SegmentCommitInfo segmentInfo;
    final FieldInfos fieldInfos;
    final FrozenBufferedUpdates segmentUpdates;
    final MutableBits liveDocs;
    final int delCount;

    private FlushedSegment(SegmentCommitInfo segmentInfo, FieldInfos fieldInfos,
                           BufferedUpdates segmentUpdates, MutableBits liveDocs, int delCount) {
      this.segmentInfo = segmentInfo;
      this.fieldInfos = fieldInfos;
      this.segmentUpdates = segmentUpdates != null && segmentUpdates.any() ? new FrozenBufferedUpdates(segmentUpdates, true) : null;
      this.liveDocs = liveDocs;
      this.delCount = delCount;
    }
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush. */
  void abort(Set<String> createdFiles) {
    //System.out.println(Thread.currentThread().getName() + ": now abort seg=" + segmentInfo.name);
    hasAborted = aborting = true;
    try {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "now abort");
      }
      try {
        consumer.abort();
      } catch (Throwable t) {
      }

      pendingUpdates.clear();
      createdFiles.addAll(directory.getCreatedFiles());
    } finally {
      aborting = false;
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "done abort");
      }
    }
  }
  private final static boolean INFO_VERBOSE = false;
  final Codec codec;
  final TrackingDirectoryWrapper directory;
  final Directory directoryOrig;
  final DocState docState;
  final DocConsumer consumer;
  final Counter bytesUsed;
  
  SegmentWriteState flushState;
  // Updates for our still-in-RAM (to be flushed next) segment
  final BufferedUpdates pendingUpdates;
  private final SegmentInfo segmentInfo;     // Current segment we are working on
  boolean aborting = false;   // True if an abort is pending
  boolean hasAborted = false; // True if the last exception throws by #updateDocument was aborting

  private FieldInfos.Builder fieldInfos;
  private final InfoStream infoStream;
  private int numDocsInRAM;
  final DocumentsWriterDeleteQueue deleteQueue;
  private final DeleteSlice deleteSlice;
  private final NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
  final Allocator byteBlockAllocator;
  final IntBlockPool.Allocator intBlockAllocator;
  private final LiveIndexWriterConfig indexWriterConfig;

  
  public DocumentsWriterPerThread(String segmentName, Directory directory, LiveIndexWriterConfig indexWriterConfig, InfoStream infoStream, DocumentsWriterDeleteQueue deleteQueue,
      FieldInfos.Builder fieldInfos) {
    this.directoryOrig = directory;
    this.directory = new TrackingDirectoryWrapper(directory);
    this.fieldInfos = fieldInfos;
    this.indexWriterConfig = indexWriterConfig;
    this.infoStream = infoStream;
    this.codec = indexWriterConfig.getCodec();
    this.docState = new DocState(this, infoStream);
    this.docState.similarity = indexWriterConfig.getSimilarity();
    bytesUsed = Counter.newCounter();
    byteBlockAllocator = new DirectTrackingAllocator(bytesUsed);
    pendingUpdates = new BufferedUpdates();
    intBlockAllocator = new IntBlockAllocator(bytesUsed);
    this.deleteQueue = deleteQueue;
    assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
    pendingUpdates.clear();
    deleteSlice = deleteQueue.newSlice();
   
    segmentInfo = new SegmentInfo(directoryOrig, Constants.LUCENE_MAIN_VERSION, segmentName, -1, false, codec, null);
    assert numDocsInRAM == 0;
    if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
      infoStream.message("DWPT", Thread.currentThread().getName() + " init seg=" + segmentName + " delQueue=" + deleteQueue);  
    }
    // this should be the last call in the ctor 
    // it really sucks that we need to pull this within the ctor and pass this ref to the chain!
    consumer = indexWriterConfig.getIndexingChain().getChain(this);

  }
  
  void setAborting() {
    aborting = true;
  }

  boolean checkAndResetHasAborted() {
    final boolean retval = hasAborted;
    hasAborted = false;
    return retval;
  }
  
  final boolean testPoint(String message) {
    if (infoStream.isEnabled("TP")) {
      infoStream.message("TP", message);
    }
    return true;
  }

  public void updateDocument(Iterable<? extends IndexableField> doc, Analyzer analyzer, Term delTerm) throws IOException {
    assert testPoint("DocumentsWriterPerThread addDocument start");
    assert deleteQueue != null;
    docState.doc = doc;
    docState.analyzer = analyzer;
    docState.docID = numDocsInRAM;
    if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
      infoStream.message("DWPT", Thread.currentThread().getName() + " update delTerm=" + delTerm + " docID=" + docState.docID + " seg=" + segmentInfo.name);
    }
    boolean success = false;
    try {
      try {
        consumer.processDocument(fieldInfos);
      } finally {
        docState.clear();
      }
      success = true;
    } finally {
      if (!success) {
        if (!aborting) {
          // mark document as deleted
          deleteDocID(docState.docID);
          numDocsInRAM++;
        } else {
          abort(filesToDelete);
        }
      }
    }
    success = false;
    try {
      consumer.finishDocument();
      success = true;
    } finally {
      if (!success) {
        abort(filesToDelete);
      }
    }
    finishDocument(delTerm);
  }

  public int updateDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer analyzer, Term delTerm) throws IOException {
    assert testPoint("DocumentsWriterPerThread addDocuments start");
    assert deleteQueue != null;
    docState.analyzer = analyzer;
    if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
      infoStream.message("DWPT", Thread.currentThread().getName() + " update delTerm=" + delTerm + " docID=" + docState.docID + " seg=" + segmentInfo.name);
    }
    int docCount = 0;
    boolean allDocsIndexed = false;
    try {
      for(Iterable<? extends IndexableField> doc : docs) {
        docState.doc = doc;
        docState.docID = numDocsInRAM;
        docCount++;

        boolean success = false;
        try {
          consumer.processDocument(fieldInfos);
          success = true;
        } finally {
          if (!success) {
            // An exc is being thrown...
            if (!aborting) {
              // Incr here because finishDocument will not
              // be called (because an exc is being thrown):
              numDocsInRAM++;
            } else {
              abort(filesToDelete);
            }
          }
        }
        success = false;
        try {
          consumer.finishDocument();
          success = true;
        } finally {
          if (!success) {
            abort(filesToDelete);
          }
        }

        finishDocument(null);
      }
      allDocsIndexed = true;

      // Apply delTerm only after all indexing has
      // succeeded, but apply it only to docs prior to when
      // this batch started:
      if (delTerm != null) {
        deleteQueue.add(delTerm, deleteSlice);
        assert deleteSlice.isTailItem(delTerm) : "expected the delete term as the tail item";
        deleteSlice.apply(pendingUpdates, numDocsInRAM-docCount);
      }

    } finally {
      if (!allDocsIndexed && !aborting) {
        // the iterator threw an exception that is not aborting 
        // go and mark all docs from this block as deleted
        int docID = numDocsInRAM-1;
        final int endDocID = docID - docCount;
        while (docID > endDocID) {
          deleteDocID(docID);
          docID--;
        }
      }
      docState.clear();
    }

    return docCount;
  }
  
  private void finishDocument(Term delTerm) {
    /*
     * here we actually finish the document in two steps 1. push the delete into
     * the queue and update our slice. 2. increment the DWPT private document
     * id.
     * 
     * the updated slice we get from 1. holds all the deletes that have occurred
     * since we updated the slice the last time.
     */
    boolean applySlice = numDocsInRAM != 0;
    if (delTerm != null) {
      deleteQueue.add(delTerm, deleteSlice);
      assert deleteSlice.isTailItem(delTerm) : "expected the delete term as the tail item";
    } else  {
      applySlice &= deleteQueue.updateSlice(deleteSlice);
    }
    
    if (applySlice) {
      deleteSlice.apply(pendingUpdates, numDocsInRAM);
    } else { // if we don't need to apply we must reset!
      deleteSlice.reset();
    }
    ++numDocsInRAM;
  }

  // Buffer a specific docID for deletion. Currently only
  // used when we hit an exception when adding a document
  void deleteDocID(int docIDUpto) {
    pendingUpdates.addDocID(docIDUpto);
    // NOTE: we do not trigger flush here.  This is
    // potentially a RAM leak, if you have an app that tries
    // to add docs but every single doc always hits a
    // non-aborting exception.  Allowing a flush here gets
    // very messy because we are only invoked when handling
    // exceptions so to do this properly, while handling an
    // exception we'd have to go off and flush new deletes
    // which is risky (likely would hit some other
    // confounding exception).
  }

  /**
   * Returns the number of delete terms in this {@link DocumentsWriterPerThread}
   */
  public int numDeleteTerms() {
    // public for FlushPolicy
    return pendingUpdates.numTermDeletes.get();
  }

  /**
   * Returns the number of RAM resident documents in this {@link DocumentsWriterPerThread}
   */
  public int getNumDocsInRAM() {
    // public for FlushPolicy
    return numDocsInRAM;
  }

  
  /**
   * Prepares this DWPT for flushing. This method will freeze and return the
   * {@link DocumentsWriterDeleteQueue}s global buffer and apply all pending
   * deletes to this DWPT.
   */
  FrozenBufferedUpdates prepareFlush() {
    assert numDocsInRAM > 0;
    final FrozenBufferedUpdates globalUpdates = deleteQueue.freezeGlobalBuffer(deleteSlice);
    /* deleteSlice can possibly be null if we have hit non-aborting exceptions during indexing and never succeeded 
    adding a document. */
    if (deleteSlice != null) {
      // apply all deletes before we flush and release the delete slice
      deleteSlice.apply(pendingUpdates, numDocsInRAM);
      assert deleteSlice.isEmpty();
      deleteSlice.reset();
    }
    return globalUpdates;
  }

  /** Flush all pending docs to a new segment */
  FlushedSegment flush() throws IOException {
    assert numDocsInRAM > 0;
    assert deleteSlice.isEmpty() : "all deletes must be applied in prepareFlush";
    segmentInfo.setDocCount(numDocsInRAM);
    final SegmentWriteState flushState = new SegmentWriteState(infoStream, directory, segmentInfo, fieldInfos.finish(),
        indexWriterConfig.getTermIndexInterval(),
        pendingUpdates, new IOContext(new FlushInfo(numDocsInRAM, bytesUsed())));
    final double startMBUsed = bytesUsed() / 1024. / 1024.;

    // Apply delete-by-docID now (delete-byDocID only
    // happens when an exception is hit processing that
    // doc, eg if analyzer has some problem w/ the text):
    if (pendingUpdates.docIDs.size() > 0) {
      flushState.liveDocs = codec.liveDocsFormat().newLiveDocs(numDocsInRAM);
      for(int delDocID : pendingUpdates.docIDs) {
        flushState.liveDocs.clear(delDocID);
      }
      flushState.delCountOnFlush = pendingUpdates.docIDs.size();
      pendingUpdates.bytesUsed.addAndGet(-pendingUpdates.docIDs.size() * BufferedUpdates.BYTES_PER_DEL_DOCID);
      pendingUpdates.docIDs.clear();
    }

    if (aborting) {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "flush: skip because aborting is set");
      }
      return null;
    }

    if (infoStream.isEnabled("DWPT")) {
      infoStream.message("DWPT", "flush postings as segment " + flushState.segmentInfo.name + " numDocs=" + numDocsInRAM);
    }

    boolean success = false;

    try {
      consumer.flush(flushState);
      pendingUpdates.terms.clear();
      segmentInfo.setFiles(new HashSet<String>(directory.getCreatedFiles()));

      final SegmentCommitInfo segmentInfoPerCommit = new SegmentCommitInfo(segmentInfo, 0, -1L, -1L);
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "new segment has " + (flushState.liveDocs == null ? 0 : (flushState.segmentInfo.getDocCount() - flushState.delCountOnFlush)) + " deleted docs");
        infoStream.message("DWPT", "new segment has " +
                           (flushState.fieldInfos.hasVectors() ? "vectors" : "no vectors") + "; " +
                           (flushState.fieldInfos.hasNorms() ? "norms" : "no norms") + "; " + 
                           (flushState.fieldInfos.hasDocValues() ? "docValues" : "no docValues") + "; " + 
                           (flushState.fieldInfos.hasProx() ? "prox" : "no prox") + "; " + 
                           (flushState.fieldInfos.hasFreq() ? "freqs" : "no freqs"));
        infoStream.message("DWPT", "flushedFiles=" + segmentInfoPerCommit.files());
        infoStream.message("DWPT", "flushed codec=" + codec);
      }

      final BufferedUpdates segmentDeletes;
      if (pendingUpdates.queries.isEmpty() && pendingUpdates.numericUpdates.isEmpty()) {
        pendingUpdates.clear();
        segmentDeletes = null;
      } else {
        segmentDeletes = pendingUpdates;
      }

      if (infoStream.isEnabled("DWPT")) {
        final double newSegmentSize = segmentInfoPerCommit.sizeInBytes()/1024./1024.;
        infoStream.message("DWPT", "flushed: segment=" + segmentInfo.name + 
                " ramUsed=" + nf.format(startMBUsed) + " MB" +
                " newFlushedSize(includes docstores)=" + nf.format(newSegmentSize) + " MB" +
                " docs/MB=" + nf.format(flushState.segmentInfo.getDocCount() / newSegmentSize));
      }

      assert segmentInfo != null;

      FlushedSegment fs = new FlushedSegment(segmentInfoPerCommit, flushState.fieldInfos,
                                             segmentDeletes, flushState.liveDocs, flushState.delCountOnFlush);
      sealFlushedSegment(fs);
      success = true;

      return fs;
    } finally {
      if (!success) {
        abort(filesToDelete);
      }
    }
  }
  
  private final Set<String> filesToDelete = new HashSet<String>(); 
  
  public Set<String> pendingFilesToDelete() {
    return filesToDelete;
  }
  /**
   * Seals the {@link SegmentInfo} for the new flushed segment and persists
   * the deleted documents {@link MutableBits}.
   */
  void sealFlushedSegment(FlushedSegment flushedSegment) throws IOException {
    assert flushedSegment != null;

    SegmentCommitInfo newSegment = flushedSegment.segmentInfo;

    IndexWriter.setDiagnostics(newSegment.info, IndexWriter.SOURCE_FLUSH);
    
    IOContext context = new IOContext(new FlushInfo(newSegment.info.getDocCount(), newSegment.sizeInBytes()));

    boolean success = false;
    try {
      
      if (indexWriterConfig.getUseCompoundFile()) {
        filesToDelete.addAll(IndexWriter.createCompoundFile(infoStream, directory, MergeState.CheckAbort.NONE, newSegment.info, context));
        newSegment.info.setUseCompoundFile(true);
      }

      // Have codec write SegmentInfo.  Must do this after
      // creating CFS so that 1) .si isn't slurped into CFS,
      // and 2) .si reflects useCompoundFile=true change
      // above:
      codec.segmentInfoFormat().getSegmentInfoWriter().write(directory, newSegment.info, flushedSegment.fieldInfos, context);

      // TODO: ideally we would freeze newSegment here!!
      // because any changes after writing the .si will be
      // lost... 

      // Must write deleted docs after the CFS so we don't
      // slurp the del file into CFS:
      if (flushedSegment.liveDocs != null) {
        final int delCount = flushedSegment.delCount;
        assert delCount > 0;
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message("DWPT", "flush: write " + delCount + " deletes gen=" + flushedSegment.segmentInfo.getDelGen());
        }

        // TODO: we should prune the segment if it's 100%
        // deleted... but merge will also catch it.

        // TODO: in the NRT case it'd be better to hand
        // this del vector over to the
        // shortly-to-be-opened SegmentReader and let it
        // carry the changes; there's no reason to use
        // filesystem as intermediary here.
          
        SegmentCommitInfo info = flushedSegment.segmentInfo;
        Codec codec = info.info.getCodec();
        codec.liveDocsFormat().writeLiveDocs(flushedSegment.liveDocs, directory, info, delCount, context);
        newSegment.setDelCount(delCount);
        newSegment.advanceDelGen();
      }

      success = true;
    } finally {
      if (!success) {
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message("DWPT",
                             "hit exception creating compound file for newly flushed segment " + newSegment.info.name);
        }
      }
    }
  }

  /** Get current segment info we are writing. */
  SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  long bytesUsed() {
    return bytesUsed.get() + pendingUpdates.bytesUsed.get();
  }

  /* Initial chunks size of the shared byte[] blocks used to
     store postings data */
  final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

  /* if you increase this, you must fix field cache impl for
   * getTerms/getTermsIndex requires <= 32768 */
  final static int MAX_TERM_LENGTH_UTF8 = BYTE_BLOCK_SIZE-2;


  private static class IntBlockAllocator extends IntBlockPool.Allocator {
    private final Counter bytesUsed;
    
    public IntBlockAllocator(Counter bytesUsed) {
      super(IntBlockPool.INT_BLOCK_SIZE);
      this.bytesUsed = bytesUsed;
    }
    
    /* Allocate another int[] from the shared pool */
    @Override
    public int[] getIntBlock() {
      int[] b = new int[IntBlockPool.INT_BLOCK_SIZE];
      bytesUsed.addAndGet(IntBlockPool.INT_BLOCK_SIZE
          * RamUsageEstimator.NUM_BYTES_INT);
      return b;
    }
    
    @Override
    public void recycleIntBlocks(int[][] blocks, int offset, int length) {
      bytesUsed.addAndGet(-(length * (IntBlockPool.INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT)));
    }
    
  }
  
  @Override
  public String toString() {
    return "DocumentsWriterPerThread [pendingDeletes=" + pendingUpdates
      + ", segment=" + (segmentInfo != null ? segmentInfo.name : "null") + ", aborting=" + aborting + ", numDocsInRAM="
        + numDocsInRAM + ", deleteQueue=" + deleteQueue + "]";
  }
  
}
