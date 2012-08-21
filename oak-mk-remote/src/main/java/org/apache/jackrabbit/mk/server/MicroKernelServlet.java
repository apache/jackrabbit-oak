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
package org.apache.jackrabbit.mk.server;

import java.io.IOException;
import java.io.PrintStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.util.MicroKernelInputStream;
import org.apache.jackrabbit.mk.util.IOUtils;

/**
 * Servlet handling requests directed at a {@code MicroKernel} instance.
 */
class MicroKernelServlet {

    /** The one and only instance of this servlet. */
    public static MicroKernelServlet INSTANCE = new MicroKernelServlet();

    /** Just one instance, no need to make constructor public */
    private MicroKernelServlet() {}

    public void service(MicroKernel mk, Request request, Response response) throws IOException {
        String file = request.getFile();
        int dotIndex = file.indexOf('.');
        if (dotIndex == -1) {
            dotIndex = file.length();
        }
        Command command = COMMANDS.get(file.substring(1, dotIndex));
        if (command != null && mk != null) {
            try {
                command.execute(mk, request, response);
            } catch (MicroKernelException e) {
                response.setStatusCode(500);
                response.setContentType("text/plain");
                e.printStackTrace(new PrintStream(response.getOutputStream()));
            } catch (Throwable e) {
                response.setStatusCode(500);
                response.setContentType("text/plain");
                e.printStackTrace(new PrintStream(response.getOutputStream()));
            }
            return;
        }
        response.setStatusCode(404);
    }

    private static interface Command {

        void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException;
    }

    private static final Map<String, Command> COMMANDS = new HashMap<String, Command>();

    static {
        COMMANDS.put("getHeadRevision", new GetHeadRevision());
        COMMANDS.put("getRevisionHistory", new GetRevisionHistory());
        COMMANDS.put("waitForCommit", new WaitForCommit());
        COMMANDS.put("getJournal", new GetJournal());
        COMMANDS.put("diff", new Diff());
        COMMANDS.put("nodeExists", new NodeExists());
        COMMANDS.put("getChildNodeCount", new GetChildNodeCount());
        COMMANDS.put("getNodes", new GetNodes());
        COMMANDS.put("commit", new Commit());
        COMMANDS.put("branch", new Branch());
        COMMANDS.put("merge", new Merge());
        COMMANDS.put("getLength", new GetLength());
        COMMANDS.put("read", new Read());
        COMMANDS.put("write", new Write());
    }

    static class GetHeadRevision implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            response.setContentType("text/plain");
            response.write(mk.getHeadRevision());
        }
    }

    static class GetRevisionHistory implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            long since = request.getParameter("since", 0L);
            int maxEntries = request.getParameter("max_entries", 10);
            String path = request.getParameter("path", "");

            response.setContentType("application/json");
            String json = mk.getRevisionHistory(since, maxEntries, path);
            if (request.getUserAgent() != null) {
                json = JsopBuilder.prettyPrint(json);
            }
            response.write(json);
        }
    }

    static class WaitForCommit implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String oldHead = request.getParameter("revision_id", headRevision);
            long maxWaitMillis = request.getParameter("max_wait_millis", 0L);

            String currentHead;

            try {
                currentHead = mk.waitForCommit(oldHead, maxWaitMillis);
            } catch (InterruptedException e) {
                throw new MicroKernelException(e);
            }

            response.setContentType("text/plain");
            response.write(currentHead == null ? "null" : currentHead);
        }
    }

    static class GetJournal implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String fromRevisionId = request.getParameter("from_revision_id", headRevision);
            String toRevisionId = request.getParameter("to_revision_id", headRevision);
            String path = request.getParameter("path", "");

            response.setContentType("application/json");
            String json = mk.getJournal(fromRevisionId, toRevisionId, path);
            if (request.getUserAgent() != null) {
                json = JsopBuilder.prettyPrint(json);
            }
            response.write(json);
        }
    }

    static class Diff implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String fromRevisionId = request.getParameter("from_revision_id", headRevision);
            String toRevisionId = request.getParameter("to_revision_id", headRevision);
            String path = request.getParameter("path", "");
            int depth = request.getParameter("depth", 1);

            response.setContentType("text/plain");
            String json = mk.diff(fromRevisionId, toRevisionId, path, depth);
            if (request.getUserAgent() != null) {
                json = JsopBuilder.prettyPrint(json);
            }
            response.write(json);
        }
    }

    static class NodeExists implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String path = request.getParameter("path", "/");
            String revisionId = request.getParameter("revision_id", headRevision);

            response.setContentType("text/plain");
            response.write(Boolean.toString(mk.nodeExists(path, revisionId)));
        }
    }

    static class GetChildNodeCount implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String path = request.getParameter("path", "/");
            String revisionId = request.getParameter("revision_id", headRevision);

            response.setContentType("text/plain");
            response.write(Long.toString(mk.getChildNodeCount(path, revisionId)));
        }
    }

    static class GetNodes implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String path = request.getParameter("path", "/");
            String revisionId = request.getParameter("revision_id", headRevision);
            int depth = request.getParameter("depth", 1);
            long offset = request.getParameter("offset", 0L);
            int maxChildNodes = request.getParameter("max_child_nodes", -1);
            String filter = request.getParameter("filter", "");

            response.setContentType("application/json");
            String json = mk.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
            // OAK-48: MicroKernel.getNodes() should return null for not existing nodes instead of throwing an exception
            if (json == null) {
                json = "null";
            }
            if (request.getUserAgent() != null) {
                json = JsopBuilder.prettyPrint(json);
            }
            response.write(json);
        }
    }

    static class Commit implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String path = request.getParameter("path", "/");
            String jsonDiff = request.getParameter("json_diff");
            String revisionId = request.getParameter("revision_id", headRevision);
            String message = request.getParameter("message");

            String newRevision = mk.commit(path, jsonDiff, revisionId, message);

            response.setContentType("text/plain");
            response.write(newRevision);
        }
    }

    static class Branch implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String headRevision = mk.getHeadRevision();

            String trunkRevisionId = request.getParameter("trunk_revision_id", headRevision);

            String newRevision = mk.branch(trunkRevisionId);

            response.setContentType("text/plain");
            response.write(newRevision);
        }
    }

    static class Merge implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String branchRevisionId = request.getParameter("branch_revision_id");
            String message = request.getParameter("message");

            String newRevision = mk.merge(branchRevisionId, message);

            response.setContentType("text/plain");
            response.write(newRevision);
        }
    }

    static class GetLength implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String blobId = request.getParameter("blob_id", "");
            long length = mk.getLength(blobId);

            response.setContentType("text/plain");
            response.write(Long.toString(length));
        }
    }

    static class Read implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            String blobId = request.getParameter("blob_id", "");
            long pos = request.getParameter("pos", 0L);
            int length = request.getParameter("length", -1);

            OutputStream out = response.getOutputStream();
            if (pos == 0L && length == -1) {
                /* return the complete binary */
                InputStream in = new MicroKernelInputStream(mk, blobId);
                IOUtils.copy(in, out);
            } else {
                /* return some range */
                byte[] buff = new byte[length];
                int count = mk.read(blobId, pos, buff, 0, length);
                if (count > 0) {
                    out.write(buff, 0, count);
                }
            }
        }
    }

    static class Write implements Command {

        @Override
        public void execute(MicroKernel mk, Request request, Response response)
                throws IOException, MicroKernelException {

            InputStream in = request.getFileParameter("file");

            String blobId = mk.write(in);

            response.setContentType("text/plain");
            response.write(blobId);
        }
    }
}
