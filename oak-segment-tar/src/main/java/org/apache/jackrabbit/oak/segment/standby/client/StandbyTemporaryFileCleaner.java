package org.apache.jackrabbit.oak.segment.standby.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

class StandbyTemporaryFileCleaner extends ChannelInboundHandlerAdapter {

    private final File spoolFolder;
    private static final Logger log = LoggerFactory.getLogger(StandbyTemporaryFileCleaner.class);

    public StandbyTemporaryFileCleaner(File spoolFolder) {
        this.spoolFolder = spoolFolder;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("StandbyTemporaryFileCleaner caught exception: ", cause);
        deleteTemporaryFilesIfExist();
        ctx.fireExceptionCaught(cause);
    }

    private void deleteTemporaryFilesIfExist() throws IOException {
        log.debug("Deleting temporary files in {}", spoolFolder);
        File[] files = spoolFolder.listFiles((dir, name) -> name.endsWith(".tmp"));

        if(files != null) {
            for(File file: files) {
                if (Files.deleteIfExists(file.toPath()))
                    log.debug("Deleted temporary file {}", file.toPath());
            }
        }
    }
}
