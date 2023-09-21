package org.apache.jackrabbit.oak.plugins.document.util;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class CompositeCommandListener implements CommandListener {
    private final List<CommandListener> listeners = new CopyOnWriteArrayList<>();

    public void addListener(CommandListener listener) {
        listeners.add(listener);
    }

    public void removeListener(CommandListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        listeners.forEach(l -> l.commandStarted(event));
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        listeners.forEach(l -> l.commandSucceeded(event));
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        listeners.forEach(l -> l.commandFailed(event));
    }
};
