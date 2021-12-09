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

package org.apache.jackrabbit.oak.segment.standby.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;
import java.security.Principal;
import java.util.regex.Pattern;

public class SSLSubjectMatcher extends ChannelInboundHandlerAdapter {

    private final Pattern pattern;

    public SSLSubjectMatcher(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext channelHandlerContext, Object ev) throws Exception {
        if (ev instanceof SslHandshakeCompletionEvent) {
            final Channel channel = channelHandlerContext.channel();
            final ChannelPipeline pipeline = channel.pipeline();
            final SslHandler ssl = (SslHandler) pipeline.get("ssl");
            final SSLSession session = ssl.engine().getSession();
            final X509Certificate x509Certificate = session.getPeerCertificateChain()[0];
            final Principal subjectDN = x509Certificate.getSubjectDN();
            final String subject = subjectDN.getName();
            if (!pattern.matcher(subject).matches()) {
                throw new Exception(
                    "Pattern match /" + pattern.toString() + "/ failed on: " + subject + "\n" +
                    "Note: the Java implementation of regex always implicitly matches from beginning to\n" +
                    "end, as in ^YOUR_PATTERN$, so if you want to match /acme/ anywhere in the subject\n" +
                    "you'd use .*acme.*");
            }
        }
    }
}
