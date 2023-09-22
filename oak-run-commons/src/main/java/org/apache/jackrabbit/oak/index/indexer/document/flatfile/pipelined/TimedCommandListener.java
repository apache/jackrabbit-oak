package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public final class TimedCommandListener implements com.mongodb.event.CommandListener {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class);
    private final String threadName;
    private final Stopwatch clock = Stopwatch.createUnstarted();

    public TimedCommandListener(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        if (isMongoDumpThread()) {
            String elapsed;
            if (clock.isRunning()) {
                elapsed = Long.toString(clock.elapsed(TimeUnit.MILLISECONDS));
            } else {
                elapsed = "N/A";
            }
            clock.reset().start();
            LOG.info("Command started: {}, processing time: {}", event.getCommand(), elapsed);
        }
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        if (isMongoDumpThread()) {
            LOG.info("Command succeeded: {}, Time: {} ms, client time: {}",
                    event.getCommandName(),
                    event.getElapsedTime(TimeUnit.MILLISECONDS),
                    clock.elapsed(TimeUnit.MILLISECONDS)
            );
            clock.reset().start();
        }
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        if (isMongoDumpThread()) {
            LOG.info("Command failed: {}", event);
        }
    }

    private boolean isMongoDumpThread() {
        return Thread.currentThread().getName().equals(threadName);
    }
}
