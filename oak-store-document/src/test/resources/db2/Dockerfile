# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM icr.io/db2_community/db2:11.5.9.0
ENV LICENSE=accept
ENV DB2INSTANCE=oak
ENV DB2INST1_PASSWORD=geheim
ENV DBNAME=
ENV BLU=false
ENV ENABLE_ORACLE_COMPATIBILITY=false
ENV UPDATEAVAIL=NO
ENV TO_CREATE_SAMPLEDB=false
ENV REPODB=false
ENV IS_OSXFS=false
ENV PERSISTENT_HOME=false
ENV HADR_ENABLED=false
ENV ETCD_ENDPOINT=
ENV ETCD_USERNAME=
ENV ETCD_PASSWORD=
RUN mkdir /var/custom
RUN echo '#!/bin/sh' >/var/custom/oak-setup.sh
RUN echo 'echo "Creating database OAK"' >>/var/custom/oak-setup.sh
RUN echo 'su -l -c "db2 create db OAK using CODESET UTF-8 TERRITORY DEFAULT COLLATE USING IDENTITY" oak' >>/var/custom/oak-setup.sh
RUN echo 'echo "Created database OAK"' >>/var/custom/oak-setup.sh
RUN chmod +x /var/custom/oak-setup.sh