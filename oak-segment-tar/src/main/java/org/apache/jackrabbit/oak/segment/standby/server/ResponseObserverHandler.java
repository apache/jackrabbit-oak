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

package org.apache.jackrabbit.oak.segment.standby.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobResponse;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentResponse;
import org.apache.jackrabbit.oak.segment.standby.store.CommunicationObserver;

/**
 * Notifies an observer when a 'get segment' or 'get blob' response is sent
 * from this server.
 */
class ResponseObserverHandler extends ChannelOutboundHandlerAdapter {

    private final CommunicationObserver observer;

    ResponseObserverHandler(CommunicationObserver observer) {
        this.observer = observer;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof GetSegmentResponse) {
            onGetSegmentResponse((GetSegmentResponse) msg);
        } else if (msg instanceof GetBlobResponse) {
            onGetBlobResponse((GetBlobResponse) msg);
        }

        ctx.write(msg, promise);
    }

    private void onGetSegmentResponse(GetSegmentResponse response) {
        observer.didSendSegmentBytes(response.getClientId(), response.getSegmentData().length);
    }

    private void onGetBlobResponse(GetBlobResponse response) {
        observer.didSendBinariesBytes(response.getClientId(), Math.max(0, response.getLength()));
    }

}
