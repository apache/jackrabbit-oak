package org.apache.jackrabbit.oak.plugins.index;



import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.ScheduleExecutionInstanceTypes.RUN_ON_LEADER;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = {})
@Designate(ocd = AsyncCheckpointService.Configuration.class, factory = true)
public class AsyncCheckpointService {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak Async Checkpoint Service",
            description = "Configures the async checkpoint services which performs periodic creation and deletion of checkpoints"
    )
    @interface Configuration {

        @AttributeDefinition(
                name = "Checkpoint Creator Identifier",
                description = "Unique identifier to be used for creating checkpoints"
        )
        String name() default "checkpoint-async";

        @AttributeDefinition(
                name = "Enable",
                description = "Flag to enable/disable the checkpoints creation task"
        )
        boolean enable() default false;

        @AttributeDefinition(
                name = "Minimum Concurrent Checkpoints",
                description = "Minimum number of concurrent checkpoints to keep in system"
        )
        long minConcurrentCheckpoints() default 2;

        @AttributeDefinition(
                name = "Checkpoint Lifetime",
                description = "Lifetime of a checkpoint in seconds"
        )
        long checkpointLifetime() default 60 * 60 * 24;

        @AttributeDefinition(
                name = "Time Interval",
                description = "Time interval between consecutive job runs in seconds. This would be the time interval between two consecutive checkpoints creation."
        )
        long timeIntervalBetweenCheckpoints() default 60 * 15;

    }

    private final List<Registration> regs = new ArrayList<>();
    @Reference
    private NodeStore nodeStore;

    @Activate
    public void activate(BundleContext bundleContext, AsyncCheckpointService.Configuration config) {
        Whiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        if (config.enable()) {
            AsyncCheckpointCreator task = new AsyncCheckpointCreator(nodeStore, config.name(), config.checkpointLifetime(), config.minConcurrentCheckpoints());
            registerAsyncCheckpointCreator(whiteboard, task, config.timeIntervalBetweenCheckpoints());
        }
    }

    private void registerAsyncCheckpointCreator(Whiteboard whiteboard, AsyncCheckpointCreator task, long delayInSeconds) {
        Map<String, Object> config = Map.of(
                AsyncCheckpointCreator.PROP_ASYNC_NAME, task.getName(),
                "scheduler.name", AsyncCheckpointCreator.class.getName() + "-" + task.getName()
        );
        regs.add(scheduleWithFixedDelay(whiteboard, task, config, delayInSeconds, RUN_ON_LEADER, true));
    }

    @Deactivate
    public void deactivate() throws IOException {
        new CompositeRegistration(regs).unregister();
    }


}
