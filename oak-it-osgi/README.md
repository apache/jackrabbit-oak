Oak Integration tests for OSGi deployments
==

This modules validates basic Oak functionality in an OSGi environment.

The Oak modules under test are not picked up from the reactor build. Instead,
they are read from the local repository and copied in a well-known location
using the maven-assembly-plugin.

In order to test a change made in a different module, you would need to:

* Run `mvn install` in the changed module
* Run `mvn assembly:single` in this module
* Re-run the test(s) in this module
