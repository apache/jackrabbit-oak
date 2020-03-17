#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
if [ -z "$1" ]; then
    echo "usage: $0 userid"
    exit
fi
ldapadd -h localhost -p 10389 -D uid=admin,ou=system -w secret <<EOF
dn: cn=Test User $1,ou=people,o=sevenSeas,dc=com
objectclass: top
objectclass: inetOrgPerson
objectclass: person
objectclass: organizationalPerson
cn: Test User $1
sn: User $1
description: User to test stuff
givenname: Test
mail: $10@mail.com
uid: $1
userpassword:: e3NoYX1uVTRlSTcxYmNuQkdxZU8wdDl0WHZZMXU1b1E9
EOF
