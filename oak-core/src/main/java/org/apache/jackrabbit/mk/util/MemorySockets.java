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
package org.apache.jackrabbit.mk.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

/**
 * Memory sockets.
 */
public abstract class MemorySockets {

    /** Sockets queue */
    static final BlockingQueue<Socket> QUEUE = new LinkedBlockingQueue<Socket>();
    
    /** Sentinel socket, used to signal a closed queue */
    static final Socket SENTINEL = new Socket();
    
    /**
     * Return the server socket factory.
     * 
     * @return server socket factory
     */
    public static ServerSocketFactory getServerSocketFactory() {
        return new ServerSocketFactory() {
            @Override
            public ServerSocket createServerSocket() throws IOException {
                return new ServerSocket() {
                    /** Closed flag */
                    private final AtomicBoolean closed = new AtomicBoolean();
                    
                    @Override
                    public Socket accept() throws IOException {
                        if (closed.get()) {
                            throw new IOException("closed");
                        }
                        try {
                            Socket socket = QUEUE.take();
                            if (socket == SENTINEL) {
                                throw new IOException("closed");
                            }
                            return socket;
                        } catch (InterruptedException e) {
                            throw new InterruptedIOException();
                        }
                    }
                    
                    @Override
                    public void close() throws IOException {
                        if (closed.compareAndSet(false, true)) {
                            QUEUE.add(SENTINEL);
                        }
                    }
                };
            }
    
            @Override
            public ServerSocket createServerSocket(int port) throws IOException {
                return createServerSocket();
            }
            
            @Override
            public ServerSocket createServerSocket(int port, int backlog)
                    throws IOException {
                
                return createServerSocket();
            }
    
            @Override
            public ServerSocket createServerSocket(int port, int backlog,
                    InetAddress ifAddress) throws IOException {
    
                return createServerSocket();
            }
        };        
    }
    
    /**
     * Return the socket factory.
     * 
     * @return socket factory
     */
    public static SocketFactory getSocketFactory() {
        return new SocketFactory() {
            @Override
            public Socket createSocket() throws IOException {
                PipedSocket socket = new PipedSocket();
                QUEUE.add(new PipedSocket(socket));
                return socket;
            }
            
            @Override
            public Socket createSocket(InetAddress host, int port) throws IOException {
                return createSocket();
            }

            @Override
            public Socket createSocket(String host, int port) throws IOException,
                    UnknownHostException {
                
                return createSocket();
            }

            @Override
            public Socket createSocket(String host, int port, InetAddress localHost,
                    int localPort) throws IOException, UnknownHostException {

                return createSocket();
            }

            @Override
            public Socket createSocket(InetAddress address, int port,
                    InetAddress localAddress, int localPort) throws IOException {

                return createSocket();
            }
        };
    };
    
    /**
     * Socket implementation, using pipes to exchange information between a
     * pair of sockets.
     */
    static class PipedSocket extends Socket {

        /** Input stream */
        protected final PipedInputStream in;

        /** Output stream */
        protected final PipedOutputStream out;
        
        /**
         * Used to initialize the socket on the client side.
         */
        PipedSocket() {
            in = new PipedInputStream(8192);
            out = new PipedOutputStream();
        }
        
        /**
         * Used to initialize the socket on the server side.
         */
        PipedSocket(PipedSocket client) throws IOException {
             in = new PipedInputStream(client.out);
             out = new PipedOutputStream(client.in);
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return in;
        }
        
        @Override
        public OutputStream getOutputStream() throws IOException {
            return out;
        }
    }
}
