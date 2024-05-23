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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

final class StandardDirectoryReader extends DirectoryReader {

  private final IndexWriter writer;
  private final SegmentInfos segmentInfos;
  private final int termInfosIndexDivisor;
  private final boolean applyAllDeletes;
  
  /** called only from static open() methods */
  StandardDirectoryReader(Directory directory, AtomicReader[] readers, IndexWriter writer,
    SegmentInfos sis, int termInfosIndexDivisor, boolean applyAllDeletes) {
    super(directory, readers);
    this.writer = writer;
    this.segmentInfos = sis;
    this.termInfosIndexDivisor = termInfosIndexDivisor;
    this.applyAllDeletes = applyAllDeletes;
  }

  /** called from DirectoryReader.open(...) methods */
  static DirectoryReader open(final Directory directory, final IndexCommit commit,
                          final int termInfosIndexDivisor) throws IOException {
    return (DirectoryReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws IOException {
        SegmentInfos sis = new SegmentInfos();
        sis.read(directory, segmentFileName);
        final SegmentReader[] readers = new SegmentReader[sis.size()];
        for (int i = sis.size()-1; i >= 0; i--) {
          IOException prior = null;
          boolean success = false;
          try {
            readers[i] = new SegmentReader(sis.info(i), termInfosIndexDivisor, IOContext.READ);
            success = true;
          } catch(IOException ex) {
            prior = ex;
          } finally {
            if (!success) {
              IOUtils.closeWhileHandlingException(prior, readers);
            }
          }
        }
        return new StandardDirectoryReader(directory, readers, null, sis, termInfosIndexDivisor, false);
      }
    }.run(commit);
  }

  /** Used by near real-time search */
  static DirectoryReader open(IndexWriter writer, SegmentInfos infos, boolean applyAllDeletes) throws IOException {
    // IndexWriter synchronizes externally before calling
    // us, which ensures infos will not change; so there's
    // no need to process segments in reverse order
    final int numSegments = infos.size();

    List<SegmentReader> readers = new ArrayList<SegmentReader>();
    final Directory dir = writer.getDirectory();

    final SegmentInfos segmentInfos = infos.clone();
    int infosUpto = 0;
    boolean success = false;
    try {
      for (int i = 0; i < numSegments; i++) {
        // NOTE: important that we use infos not
        // segmentInfos here, so that we are passing the
        // actual instance of SegmentInfoPerCommit in
        // IndexWriter's segmentInfos:
        final SegmentCommitInfo info = infos.info(i);
        assert info.info.dir == dir;
        final ReadersAndUpdates rld = writer.readerPool.get(info, true);
        try {
          final SegmentReader reader = rld.getReadOnlyClone(IOContext.READ);
          if (reader.numDocs() > 0 || writer.getKeepFullyDeletedSegments()) {
            // Steal the ref:
            readers.add(reader);
            infosUpto++;
          } else {
            reader.decRef();
            segmentInfos.remove(infosUpto);
          }
        } finally {
          writer.readerPool.release(rld);
        }
      }
      
      writer.incRefDeleter(segmentInfos);
      
      StandardDirectoryReader result = new StandardDirectoryReader(dir,
          readers.toArray(new SegmentReader[readers.size()]), writer,
          segmentInfos, writer.getConfig().getReaderTermsIndexDivisor(), applyAllDeletes);
      success = true;
      return result;
    } finally {
      if (!success) {
        for (SegmentReader r : readers) {
          try {
            r.decRef();
          } catch (Throwable th) {
            // ignore any exception that is thrown here to not mask any original
            // exception. 
          }
        }
      }
    }
  }

  /** This constructor is only used for {@link #doOpenIfChanged(SegmentInfos)} */
  private static DirectoryReader open(Directory directory, SegmentInfos infos, List<? extends AtomicReader> oldReaders,
    int termInfosIndexDivisor) throws IOException {

    // we put the old SegmentReaders in a map, that allows us
    // to lookup a reader using its segment name
    final Map<String,Integer> segmentReaders = new HashMap<String,Integer>();

    if (oldReaders != null) {
      // create a Map SegmentName->SegmentReader
      for (int i = 0, c = oldReaders.size(); i < c; i++) {
        final SegmentReader sr = (SegmentReader) oldReaders.get(i);
        segmentReaders.put(sr.getSegmentName(), Integer.valueOf(i));
      }
    }
    
    SegmentReader[] newReaders = new SegmentReader[infos.size()];
    
    // remember which readers are shared between the old and the re-opened
    // DirectoryReader - we have to incRef those readers
    boolean[] readerShared = new boolean[infos.size()];
    
    for (int i = infos.size() - 1; i>=0; i--) {
      // find SegmentReader for this segment
      Integer oldReaderIndex = segmentReaders.get(infos.info(i).info.name);
      if (oldReaderIndex == null) {
        // this is a new segment, no old SegmentReader can be reused
        newReaders[i] = null;
      } else {
        // there is an old reader for this segment - we'll try to reopen it
        newReaders[i] = (SegmentReader) oldReaders.get(oldReaderIndex.intValue());
      }

      boolean success = false;
      Throwable prior = null;
      try {
        SegmentReader newReader;
        if (newReaders[i] == null || infos.info(i).info.getUseCompoundFile() != newReaders[i].getSegmentInfo().info.getUseCompoundFile()) {

          // this is a new reader; in case we hit an exception we can close it safely
          newReader = new SegmentReader(infos.info(i), termInfosIndexDivisor, IOContext.READ);
          readerShared[i] = false;
          newReaders[i] = newReader;
        } else {
          if (newReaders[i].getSegmentInfo().getDelGen() == infos.info(i).getDelGen()
              && newReaders[i].getSegmentInfo().getFieldInfosGen() == infos.info(i).getFieldInfosGen()) {
            // No change; this reader will be shared between
            // the old and the new one, so we must incRef
            // it:
            readerShared[i] = true;
            newReaders[i].incRef();
          } else {
            // there are changes to the reader, either liveDocs or DV updates
            readerShared[i] = false;
            // Steal the ref returned by SegmentReader ctor:
            assert infos.info(i).info.dir == newReaders[i].getSegmentInfo().info.dir;
            assert infos.info(i).hasDeletions() || infos.info(i).hasFieldUpdates();
            if (newReaders[i].getSegmentInfo().getDelGen() == infos.info(i).getDelGen()) {
              // only DV updates
              newReaders[i] = new SegmentReader(infos.info(i), newReaders[i], newReaders[i].getLiveDocs(), newReaders[i].numDocs());
            } else {
              // both DV and liveDocs have changed
              newReaders[i] = new SegmentReader(infos.info(i), newReaders[i]);
            }
          }
        }
        success = true;
      } catch (Throwable ex) {
        prior = ex;
      } finally {
        if (!success) {
          for (i++; i < infos.size(); i++) {
            if (newReaders[i] != null) {
              try {
                if (!readerShared[i]) {
                  // this is a new subReader that is not used by the old one,
                  // we can close it
                  newReaders[i].close();
                } else {
                  // this subReader is also used by the old reader, so instead
                  // closing we must decRef it
                  newReaders[i].decRef();
                }
              } catch (Throwable t) {
                if (prior == null) prior = t;
              }
            }
          }
        }
        // throw the first exception
        IOUtils.reThrow(prior);
      }
    }    
    return new StandardDirectoryReader(directory, newReaders, null, infos, termInfosIndexDivisor, false);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final String segmentsFile = segmentInfos.getSegmentsFileName();
    if (segmentsFile != null) {
      buffer.append(segmentsFile).append(":").append(segmentInfos.getVersion());
    }
    if (writer != null) {
      buffer.append(":nrt");
    }
    for (final AtomicReader r : getSequentialSubReaders()) {
      buffer.append(' ');
      buffer.append(r);
    }
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  protected DirectoryReader doOpenIfChanged() throws IOException {
    return doOpenIfChanged((IndexCommit) null);
  }

  @Override
  protected DirectoryReader doOpenIfChanged(final IndexCommit commit) throws IOException {
    ensureOpen();

    // If we were obtained by writer.getReader(), re-ask the
    // writer to get a new reader.
    if (writer != null) {
      return doOpenFromWriter(commit);
    } else {
      return doOpenNoWriter(commit);
    }
  }

  @Override
  protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
    ensureOpen();
    if (writer == this.writer && applyAllDeletes == this.applyAllDeletes) {
      return doOpenFromWriter(null);
    } else {
      return writer.getReader(applyAllDeletes);
    }
  }

  private DirectoryReader doOpenFromWriter(IndexCommit commit) throws IOException {
    if (commit != null) {
      return doOpenFromCommit(commit);
    }

    if (writer.nrtIsCurrent(segmentInfos)) {
      return null;
    }

    DirectoryReader reader = writer.getReader(applyAllDeletes);

    // If in fact no changes took place, return null:
    if (reader.getVersion() == segmentInfos.getVersion()) {
      reader.decRef();
      return null;
    }

    return reader;
  }

  private DirectoryReader doOpenNoWriter(IndexCommit commit) throws IOException {

    if (commit == null) {
      if (isCurrent()) {
        return null;
      }
    } else {
      if (directory != commit.getDirectory()) {
        throw new IOException("the specified commit does not match the specified Directory");
      }
      if (segmentInfos != null && commit.getSegmentsFileName().equals(segmentInfos.getSegmentsFileName())) {
        return null;
      }
    }

    return doOpenFromCommit(commit);
  }

  private DirectoryReader doOpenFromCommit(IndexCommit commit) throws IOException {
    return (DirectoryReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws IOException {
        final SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName);
        return doOpenIfChanged(infos);
      }
    }.run(commit);
  }

  DirectoryReader doOpenIfChanged(SegmentInfos infos) throws IOException {
    return StandardDirectoryReader.open(directory, infos, getSequentialSubReaders(), termInfosIndexDivisor);
  }

  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  @Override
  public boolean isCurrent() throws IOException {
    ensureOpen();
    if (writer == null || writer.isClosed()) {
      // Fully read the segments file: this ensures that it's
      // completely written so that if
      // IndexWriter.prepareCommit has been called (but not
      // yet commit), then the reader will still see itself as
      // current:
      SegmentInfos sis = new SegmentInfos();
      sis.read(directory);

      // we loaded SegmentInfos from the directory
      return sis.getVersion() == segmentInfos.getVersion();
    } else {
      return writer.nrtIsCurrent(segmentInfos);
    }
  }

  @Override
  protected void doClose() throws IOException {
    Throwable firstExc = null;
    for (final AtomicReader r : getSequentialSubReaders()) {
      // try to close each reader, even if an exception is thrown
      try {
        r.decRef();
      } catch (Throwable t) {
        if (firstExc == null) {
          firstExc = t;
        }
      }
    }

    if (writer != null) {
      try {
        writer.decRefDeleter(segmentInfos);
      } catch (AlreadyClosedException ex) {
        // This is OK, it just means our original writer was
        // closed before we were, and this may leave some
        // un-referenced files in the index, which is
        // harmless.  The next time IW is opened on the
        // index, it will delete them.
      }
    }

    // throw the first exception
    IOUtils.reThrow(firstExc);
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    ensureOpen();
    return new ReaderCommit(segmentInfos, directory);
  }

  static final class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection<String> files;
    Directory dir;
    long generation;
    final Map<String,String> userData;
    private final int segmentCount;

    ReaderCommit(SegmentInfos infos, Directory dir) throws IOException {
      segmentsFileName = infos.getSegmentsFileName();
      this.dir = dir;
      userData = infos.getUserData();
      files = Collections.unmodifiableCollection(infos.files(dir, true));
      generation = infos.getGeneration();
      segmentCount = infos.size();
    }

    @Override
    public String toString() {
      return "DirectoryReader.ReaderCommit(" + segmentsFileName + ")";
    }

    @Override
    public int getSegmentCount() {
      return segmentCount;
    }

    @Override
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    @Override
    public Collection<String> getFileNames() {
      return files;
    }

    @Override
    public Directory getDirectory() {
      return dir;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public boolean isDeleted() {
      return false;
    }

    @Override
    public Map<String,String> getUserData() {
      return userData;
    }

    @Override
    public void delete() {
      throw new UnsupportedOperationException("This IndexCommit does not support deletions");
    }
  }
}
