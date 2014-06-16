Oak PojoSR
==========

This module demonstrates running Oak outside of OSGi environments but using in built OSGi
support for configuring Oak. It makes use of [PojoSR][1] to provide the OSGi framework
support. This would enable usage of Oak in POJO env and would still enable usage of OSGi features
to customize Oak components.

To make use of this following dependencies are required

1. PojoSR - Provides the OSGi framework support
2. Apache Felix SCR
3. Apache Felix Config Admin
4. Apache Felix Fileinstall - To provision configuration

[1]: https://code.google.com/p/pojosr/