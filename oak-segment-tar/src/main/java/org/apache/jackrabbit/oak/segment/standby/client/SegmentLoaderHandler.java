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
package org.apache.jackrabbit.oak.segment.standby.client;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.segment.standby.codec.Messages.newGetBlobReq;
import static org.apache.jackrabbit.oak.segment.standby.codec.Messages.newGetSegmentReq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.standby.codec.SegmentReply;
import org.apache.jackrabbit.oak.segment.standby.store.RemoteSegmentLoader;
import org.apache.jackrabbit.oak.segment.standby.store.StandbyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentLoaderHandler extends ChannelInboundHandlerAdapter implements RemoteSegmentLoader {

    private static final Logger log = LoggerFactory.getLogger(SegmentLoaderHandler.class);

    private final StandbyStore store;
    private final String clientID;
    private final RecordId head;
    private final AtomicBoolean running;
    private final int readTimeoutMs;
    private final boolean autoClean;

    private volatile ChannelHandlerContext ctx;

    private final BlockingQueue<SegmentReply> segment = new LinkedBlockingQueue<SegmentReply>();

    // Use a separate thread for sync'ing. Leave the I/O thread free to process
    // I/O requests.
    private ExecutorService syncExecutor;

    public SegmentLoaderHandler(StandbyStore store, RecordId head, String clientID, AtomicBoolean running, int readTimeoutMs, boolean autoClean) {
        this.store = store;
        this.head = head;
        this.clientID = clientID;
        this.running = running;
        this.readTimeoutMs = readTimeoutMs;
        this.autoClean = autoClean;
        this.syncExecutor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception caught, closing channel.", cause);
        close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        syncExecutor.shutdown();
        syncExecutor = null;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof String) {
            onCommand((String) evt);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof SegmentReply) {
            onSegmentReply((SegmentReply) msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void onSegmentReply(SegmentReply reply) {
        // Offer the reply from the I/O thread, unblocking the sync thread.
        segment.offer(reply);
    }

    private void onCommand(String command) {
        if (command.equals("sync")) {
            syncExecutor.submit(new Runnable() {

                @Override
                public void run() {
                    sync();
                }

            });
        }
    }

    private void sync() {
        log.debug("new head id " + head);
        long t = System.currentTimeMillis();
        long preSyncSize = -1;
        if (autoClean) {
            preSyncSize = store.size();
        }

        try {
            store.preSync(this);
            SegmentNodeState before = store.getHead();
            SegmentNodeBuilder builder = before.builder();

            SegmentNodeState current = store.newSegmentNodeState(head);
            do {
                try {
                    current.compareAgainstBaseState(before,
                            new StandbyApplyDiff(builder, store, this));
                    break;
                } catch (SegmentNotFoundException e) {
                    // the segment is locally damaged or not present anymore
                    // lets try to read this from the primary again
                    String id = e.getSegmentId();
                    Segment s = readSegment(e.getSegmentId());
                    if (s == null) {
                        log.warn("can't read locally corrupt segment " + id + " from primary");
                        throw e;
                    }

                    log.debug("did reread locally corrupt segment " + id + " with size " + s.size());
                    store.persist(s.getSegmentId(), s);
                }
            } while(true);
            boolean ok = store.setHead(before, builder.getNodeState());
            log.debug("updated head state successfully: {} in {}ms.", ok,
                    System.currentTimeMillis() - t);

            if (autoClean && preSyncSize > 0) {
                long postSyncSize = store.size();
                // if size gain is over 25% call cleanup
                if (postSyncSize - preSyncSize > 0.25 * preSyncSize) {
                    log.info(
                            "Store size increased from {} to {}, will run cleanup.",
                            humanReadableByteCount(preSyncSize),
                            humanReadableByteCount(postSyncSize));
                    store.cleanup();
                }
            }
        } finally {
            store.postSync();
            close();
        }
    }

    @Override
    public Segment readSegment(final String id) {
        // Use the I/O thread to write the request to the server
        ctx.writeAndFlush(newGetSegmentReq(this.clientID, id));
        // Wait on the sync thread for the response.
        return getSegment(id);
    }

    @Override
    public Blob readBlob(String blobId) {
        // Use the I/O thread to write the request to the server
        ctx.writeAndFlush(newGetBlobReq(this.clientID, blobId));
        // Wait on the sync thread for the response.
        return getBlob(blobId);
    }

    private Segment getSegment(final String id) {
        return getReply(id, SegmentReply.SEGMENT).getSegment();
    }

    private Blob getBlob(final String id) {
        return getReply(id, SegmentReply.BLOB).getBlob();
    }

    private SegmentReply getReply(final String id, int type) {
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    // Block the sync thread for a response from the server.
                    SegmentReply r = segment.poll(readTimeoutMs, TimeUnit.MILLISECONDS);

                    if (r == null) {
                        log.warn("timeout waiting for {}", id);
                        return SegmentReply.empty();
                    }

                    if (r.getType() == type) {
                        switch (r.getType()) {
                        case SegmentReply.SEGMENT:
                            if (r.getSegment().getSegmentId().toString()
                                    .equals(id)) {
                                return r;
                            }
                            break;
                        case SegmentReply.BLOB:
                            if (r.getBlob().getBlobId().equals(id)) {
                                return r;
                            }
                            break;
                        }
                    }
                } catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        ctx.close();
    }

    @Override
    public boolean isClosed() {
        return !ctx.channel().isActive();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

}
