#!/usr/bin/perl
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

#
# script to generate a LDIF with 66 primary groups and 17,576 unique users.
# There are additional 100 secondary groups, where indirectly all users are member of.
# Each user is a member of about 6 or 7 groups.
# The largest groups contain about 20% of the users (~3,500 users)
# The users all have the same password "password"
#
# The oak-users.csv file contains all of the users. Here is an example of the data from this CSV:
#
# 3997,T0F9D,Told Xyla
# 3998,U0F9E,Upshove Xerasia
# 3999,V0F9F,Verve Xenicus
# 4000,W0FA0,Wharf Xiphiid
# 4001,X0FA1,Xyletic Xenial
# 4002,Y0FA2,Yean Xylonic
# 4003,Z0FA3,Zimocca Xylenol
# 4004,A0FA4,Agnail Yukian
# 4005,B0FA5,Boolian Yapman
#
# The columns are:
# <sequential-number>,<user-id>,<users-full-name>
#
# Group membership for each user is based on the sequential number and the first letters of the first and last full-name.
# The first letter of the names defined the user as being in groups "has_<letter>" for each part of the full name
# Each user is part of groups:
# <modulus>_of_five
# <modulus>_of_seven
# <modulus>_of_eleven
# <modulus>_of_seventeen
#
# According to the user's <sequential-number> so that <modulus> = <sequential-number> % 5, 7, 11 and 17.
# For example, uid=T0F9D (sequential-number=3997) is a member of groups two_of_five, zero_of_seven,
# four_of_eleven and two_of_seventeen
#
# Alas, there is no seven_of_nine.
#

%allgroups = ();

$base = "o=oak,dc=apache,dc=org";

# 0,A0000,Adeem Anend
# 1,B0001,Brenda Abashed

@digits = ( "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
            "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen",
            "seventeen", "eighteen", "nineteen"  );

sub n_of_m {
  local ( $num, $mod ) = @_;
  local $n, $g;
  $n = $num % $mod;
  $g = @digits[$n] . "_of_" . @digits[$mod];
  return $g;
}

sub do_group {
  local ( $group ) = @_;

  unless (defined($allgroups{$group})) {
     $mod = <<"DONE";
# define group $group
dn: cn=$group,ou=Groups,$base
changetype: add
objectclass: top
objectclass: groupOfUniqueNames
cn: $group
uniquemember: cn=$name,ou=Users,$base

DONE
     print $mod;
  } else {

  $mod = <<"DONE";
# modify group $group
dn: cn=$group,ou=Groups,$base
changetype: modify
add: uniquemember
uniquemember: cn=$name,ou=Users,$base

DONE

  print $mod;
}
  $allgroups{$group} .= $uid;
}

print <<"DONE";
dn: ou=Groups,$base
objectclass: top
objectclass: organizationalUnit
ou: groups

dn: ou=Users,$base
objectclass: top
objectclass: organizationalUnit
ou: Users

dn: cn=admin,$base
objectclass: person
objectclass: organizationalPerson
objectclass: inetOrgPerson
objectclass: top
uid: admin
cn: Administrator
displayName: Administrator
givenName: Administrator
sn: Administrator
description: Administrator of test company
mail: admin\@oak.example.org
userPassword: password

DONE

while (<>) {
  chomp;
  $_ =~ s/\s+$//;

  @fields = split /,/;

  $num = @fields[0];
  $id = lc @fields[1];
  $name = @fields[2];
  $name =~ /^([^ ]+)\s+([^ ]+)$/;
  $fname = $1;
  $lname = $2;


$g = &n_of_m($num, 5);
$g = $g.",".&n_of_m($num, 7);
$g = $g.",".&n_of_m($num, 11);
$g = $g.",".&n_of_m($num, 17);

$fname =~/^(\w)/; $l1 = $1; $l1 =~ y/A-Z/a-z/;
$lname =~/^(\w)/; $l2 = $1; $l2 =~ y/A-Z/a-z/;
$g = $g.",has_$l1";
$g = $g.",has_$l2" unless ($l1 eq $l2);

#  print "num=$num id=$id name=$name fname=$fname lname=$lname groups=$g\n";

  $ldif = <<"DONE";
# $num,$id,$name
# groups: $g
#
dn: cn=$name,ou=Users, $base
objectclass: person
objectclass: organizationalPerson
objectclass: inetOrgPerson
objectclass: top
uid: $id
cn: $name
displayName: $name
givenName: $fname
sn: $lname
description: $name is a test user with groups $g
mail: $id\@oak.example.org
userPassword: password

DONE

  print $ldif;

  foreach $i (split /,/, $g) {
     do_group($i);
  }
}

foreach my $i (1..100) {
print <<"DONE";

dn: cn=dummy_$i,ou=Groups,$base
changetype: add
objectclass: top
objectclass: groupOfUniqueNames
cn: dummy_$i
DONE

    foreach my $i ('a'..'z') {
        print "uniquemember: cn=has_$i,ou=Groups,$base\n"
    }
}
