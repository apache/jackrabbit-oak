<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

## Atomic Counter
`@since 1.3.0 (stand-alone) , 1.3.14 (full cluster support)`

### Overview

The atomic counter functionality aims to address the need of use cases
like _votes_, _likes_, _+1s_ and so on. It will make sure you'll
eventually have a consistent and correct account of the
sums/subtractions of all the increments.

When you set a specific node type (`mix:atomicCounter`) to any node,
you'll have a protected property, `oak:counter` that will hold the
count of your votes.

To perform an increment or decrement you'll have to set a specific
property of type `Long`: `oak:increment`. See later on in the
[usage section](#Usage).

#### Stand-alone, synchronous

When running on stand-alone configurations, like Segment, the actual
increase of the `oak:counter` property will happen synchronously, in
the same space of the commit. Therefore it will always reflect an up
to date value.

#### Clustered, asynchronous

When running on clustered solutions, or potential one, like on
DocumentMK the actual increase of the `oak:counter` property will
happen asynchronously. Therefore the value displayed by `oak:counter`
could not be up to date and lagging behind of some time. This is for
dealing with conflicts that could happen when updating the same
properties across the cluster and for scaling without having to deal
with any global locking system.

The consolidation task will timeout by default at `32000ms`. This
aspect is configurable by providing the environment variable:
`oak.atomiccounter.task.timeout`. In case a node time out it will be
tracked in the logs with a warning.

For example to increase the timeout up to 64 seconds you can set from
the command line `-Doak.atomiccounter.task.timeout=64000`.

##### Constraints

For the clustered solution, in order to have the asynchronous
behaviour enabled you **have to provide** a
`org.apache.jackrabbit.oak.spi.state.Clusterable` and a
`java.util.concurrent.ScheduledExecutorService`. If any of these are
null it will fall back to a synchronous behaviour.

It will fall back to synchronous as well if no
`org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard` is provided or
the provided commit hooks during repository constructions are not
registered with the whiteboard itself. These are done automatically by
the default Oak repository constructions, so other that you customise
further no actions should be needed.

### Enabling the feature (Repository Construction)
#### Plain Java

##### Stand-alone

    NodeStore store = ... //store instantiation
    Jcr jcr = new Jcr(store).withAtomicCounter();
    Repository repo = jcr.createContentRepository();

##### Clustered

    NodeStore store = ... //store instantiation

    // DocumentNodeStore implements such aspect therefore it could be
    // something like: `(Clusterable) store`. Casting the store into
    // Clusterable.
    Clusterable clusterable = ...

    // you have to provide a ScheduledExecutorService which you'll
    // have to take care of shutting it down properly during
    // repository shutdown.
    ScheduledExecutorService executor = ...

    Jcr jcr = new Jcr(store)
        .with(clusterable)
        .with(executor)
        .withAtomicCounter();

    Repository repo = jcr.createContentRepository();

#### OSGi

##### Stand-alone and Clustered

    @Reference(target = "(type=atomicCounter)")
    private EditorProvider atomicCounter;
    
    ...
    
    NodeStore store = ...
    Jcr jcr = new Jcr(store);
    jcr.with(atomicCounter);
    
    ...

When running on clustered environment the `EditorProvider` expect to
find a service of type
`org.apache.jackrabbit.oak.spi.state.Clusterable` and
`org.apache.jackrabbit.oak.spi.state.NodeStore`. `DocumentNodeStore`
already register itself as `Clusterable`. If one of the two won't be
available it will fall back to synchronous behaviour.

### Usage

    Session session = ...
  
    // creating a counter node
    Node counter = session.getRootNode().addNode("mycounter");
    counter.addMixin("mix:atomicCounter"); // or use the NodeTypeConstants
    session.save();
  
    // Will output 0. the default value
    System.out.println("counter now: " + counter.getProperty("oak:counter").getLong());
  
    // incrementing by 5 the counter
    counter.setProperty("oak:increment", 5);
    session.save();
  
    // Will output 5
    System.out.println("counter now: " + counter.getProperty("oak:counter").getLong());
  
    // decreasing by 1
    counter.setProperty("oak:increment", -1);
    session.save();
  
    // Will output 4
    System.out.println("counter now: " + counter.getProperty("oak:counter").getLong());
  
    session.logout();
 
### Debug

If you're experiencing any problems with the counter you can start
analysing the situation by setting to `DEBUG` log appender
`org.apache.jackrabbit.oak.plugins.atomic`.

If set to `TRACE` even more information will be provided.

