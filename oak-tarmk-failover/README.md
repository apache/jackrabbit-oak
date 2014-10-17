Oak TarMK Failover
==================

Failover
-------

The component should be installed when failover support is needed.

The setup is expected to be: one master to one/many slaves nodes.
The slave will periodically poll the master for the head state, if this
changed, it will pull in all the new segments since the last sync.

Setup in OSGi
-------------

The FailoverStoreService will be enabled if there is a config available for the component.
Expected setup is:
  - for the 'master' mode: mode=master (default), port=8023 (default)
  - for the 'slave' mode:  mode=slave, port=8023 (default), master.host=127.0.0.1 (default), interval=5 (default)

Port is used in both configs: for master is the port where the server will be available on the host, for the slave it 
represents the port on the master it needs to connect to.
Master host represents the master host info.
Interval represents how often the sync thread should run, in seconds.

See examples in the osgi-conf folder for each run mode. To install a new OSGI config in the sling launcher,
you only need to create a new folder called 'install' in the sling.home folder and copy the specific config there.

TODO
----

  - timeout handling doesn't cover everything on both server and slave
  - error handling on the slave still has some issues (the slave hangs)
  - maybe enable compression of the segments over the wire
  - slave runmode could possibly be a read-only mode (no writes permitted)

License
-------

(see the top-level [LICENSE.txt](../LICENSE.txt) for full license details)

Collective work: Copyright 2012 The Apache Software Foundation.

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

