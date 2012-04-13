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
package org.apache.jackrabbit.mk.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.SocketFactory;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.util.IOUtils;

/**
 * Client exposing a <code>MicroKernel</code> interface, that "remotes" commands
 * to a server.
 * <p/>
 * All public methods inside this class are completely synchronized because
 * HttpExecutor is not thread-safe.
 */
public class Client implements MicroKernel {
    
    private static final String MK_EXCEPTION_PREFIX = MicroKernelException.class.getName() + ":";  
    
    private final InetSocketAddress addr;
    
    private final SocketFactory socketFactory;

    private final AtomicBoolean disposed = new AtomicBoolean();
    
    private HttpExecutor executor;

    /**
     * Returns the socket address of the given URL.
     * 
     * @param url URL
     * @return socket address
     */
    private static InetSocketAddress getAddress(String url) {
        try {
            URI uri = new URI(url);
            return new InetSocketAddress(uri.getHost(), uri.getPort());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
    

    /**
     * Create a new instance of this class.
     * 
     * @param url socket address
     */
    public Client(String url) {
        this(getAddress(url));
    }

    /**
     * Create a new instance of this class.
     * 
     * @param addr socket address
     */
    public Client(InetSocketAddress addr) {
        this(addr, SocketFactory.getDefault());
    }

    /**
     * Create a new instance of this class.
     * 
     * @param addr socket address
     */
    public Client(InetSocketAddress addr, SocketFactory socketFactory) {
        this.addr = addr;
        this.socketFactory = socketFactory;
    }
    
    //-------------------------------------------------- implements MicroKernel
    
    public synchronized void dispose() {
        if (!disposed.compareAndSet(false, true)) {
            return;
        }
        IOUtils.closeQuietly(executor);
    }

    public synchronized String getHeadRevision() throws MicroKernelException {
        Request request = null;
        
        try {
            request = createRequest("getHeadRevision");
            return request.getString();
        } catch (IOException e) {
            throw new MicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized String getRevisions(long since, int maxEntries)
            throws MicroKernelException {

        Request request = null;

        try {
            request = createRequest("getRevisions");
            request.addParameter("since", since);
            request.addParameter("max_entries", maxEntries);
            return request.getString();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized String waitForCommit(String oldHeadRevisionId, long maxWaitMillis)
            throws MicroKernelException, InterruptedException {

        Request request = null;

        try {
            request = createRequest("waitForCommit");
            request.addParameter("revision_id", oldHeadRevisionId);
            request.addParameter("max_wait_millis", maxWaitMillis);
            return request.getString();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized String getJournal(String fromRevisionId, String toRevisionId, String filter)
            throws MicroKernelException {
        
        Request request = null;

        try {
            request = createRequest("getJournal");
            request.addParameter("from_revision_id", fromRevisionId);
            request.addParameter("to_revision_id", toRevisionId);
            request.addParameter("filter", filter);
            return request.getString();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized String diff(String fromRevisionId, String toRevisionId, String filter)
            throws MicroKernelException {
        Request request = null;

        try {
            request = createRequest("diff");
            request.addParameter("from_revision_id", fromRevisionId);
            request.addParameter("to_revision_id", toRevisionId);
            request.addParameter("filter", filter);
            return request.getString();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {

        Request request = null;

        try {
            request = createRequest("nodeExists");
            request.addParameter("path", path);
            request.addParameter("revision_id", revisionId);
            return request.getBoolean();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {

        Request request = null;

        try {
            request = createRequest("getChildNodeCount");
            request.addParameter("path", path);
            request.addParameter("revision_id", revisionId);
            return request.getLong();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized String getNodes(String path, String revisionId)
            throws MicroKernelException {

        return getNodes(path, revisionId, 1, 0, -1, null);
    }

    public synchronized String getNodes(String path, String revisionId, int depth,
            long offset, int count, String filter) throws MicroKernelException {
        
        Request request = null;

        try {
            request = createRequest("getNodes");
            request.addParameter("path", path);
            request.addParameter("revision_id", revisionId);
            request.addParameter("depth", depth);
            request.addParameter("offset", offset);
            request.addParameter("count", count);
            request.addParameter("filter", filter);
            // OAK-48: MicroKernel.getNodes() should return null for not existing nodes instead of throwing an exception
            String result = request.getString();
            return result.equals("null") ? null : result;
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized String commit(String path, String jsonDiff, String revisionId, 
            String message) throws MicroKernelException {
        
        Request request = null;

        try {
            request = createRequest("commit");
            request.addParameter("path", path);
            request.addParameter("json_diff", jsonDiff);
            request.addParameter("revision_id", revisionId);
            request.addParameter("message", message);
            return request.getString();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized long getLength(String blobId) throws MicroKernelException {
        Request request = null;

        try {
            request = createRequest("getLength");
            request.addParameter("blob_id", blobId);
            return request.getLong();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {

        Request request = null;

        try {
            request = createRequest("read");
            request.addParameter("blob_id", blobId);
            request.addParameter("pos", pos);
            request.addParameter("length", length);
            return request.read(buff, off, length);
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }

    public synchronized String write(InputStream in) throws MicroKernelException {
        Request request = null;

        try {
            request = createRequest("write");
            request.addFileParameter("file", in);
            return request.getString();
        } catch (IOException e) {
            throw toMicroKernelException(e);
        } finally {
            IOUtils.closeQuietly(request);
        }
    }
    
    /**
     * Convert an I/O exception into a MicroKernelException, possibly by 
     * unwrapping an already wrapped MicroKernelException.
     * 
     * @param e I/O exception 
     * @return MicroKernelException
     */
    private MicroKernelException toMicroKernelException(IOException e) {
        String msg = e.getMessage();
        if (msg != null && msg.startsWith(MK_EXCEPTION_PREFIX)) {
            return new MicroKernelException(msg.substring(MK_EXCEPTION_PREFIX.length()).trim());
        }
        return new MicroKernelException(e);
    }

    /**
     * Create a request for the given command to be executed.
     * 
     * @param command command name
     * @return request
     * @throws IOException if an I/O error occurs
     * @throws MicroKernelException if an exception occurs
     */
    private Request createRequest(String command) throws IOException, MicroKernelException {
        if (disposed.get()) {
            throw new IllegalStateException("This instance has already been disposed");
        }
        if (executor != null && !executor.isAlive()) {
            IOUtils.closeQuietly(executor);
            executor = null;
        }
        if (executor == null) {
            executor = new HttpExecutor(createSocket());
        }
        return new Request(executor, command);
    }
    
    private Socket createSocket() throws IOException {
        if (addr == null) {
            return socketFactory.createSocket();
        }
        return socketFactory.createSocket(addr.getAddress(), addr.getPort());
    }
}
