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

my %traces = ();
my $root = { 'count' => 0, 'frame' => 'root', 'children' => {} };
my $leaf = { 'count' => 0, 'frame' => 'leaf', 'children' => {} };

sub accumulate {
  my ($trace, $count) = @_;

  $root->{'count'} += $count;
  my $info = $root;
  for my $frame (@{$traces{$trace}}) {
    my $children = $info->{'children'};
    $children->{$frame} or $children->{$frame} = {
      'count' => 0,
      'frame' => $frame,
      'children' => {}
    };
    $info = $children->{$frame};
    $info->{'count'} += $count;
  }

  $leaf->{'count'} += $count;
  my $info = $leaf;
  for my $frame (reverse @{$traces{$trace}}) {
    my $children = $info->{'children'};
    $children->{$frame} or $children->{$frame} = {
      'count' => 0,
      'frame' => $frame,
      'children' => {}
    };
    $info = $children->{$frame};
    $info->{'count'} += $count;
  }
}

sub output {
  my ($total, $info, $prefix, $limit) = @_;

  my $children = $info->{'children'};
  for my $child (sort { $b->{'count'} - $a->{'count'} } values %{$children}) {
    my $frame = $child->{'frame'};
    my $ratio = int(100 * $child->{'count'} / $total);
    if ($ratio > 1) {
      printf "%s%.1d%% %s\n", $prefix, $ratio, $frame;
      $limit and output($total, $child, "$prefix ", $limit - 1);
    }
  }
}

my $trace = '';
while (<>) {
  /^TRACE (\d+)/  and $trace = $1 and $traces{$trace} = [] and next;
  /^\s+([a-z].*?)(\(.*)?\r?\n/ and $trace and $traces{$trace} = [ $1, @{$traces{$trace}} ] and next;
  /^rank/ and $trace and $trace = '' and next;
  /^\s*\d+\s+\S+%\s+\S+%\s+(\d+)\s+(\d+)/ and accumulate($2, $1); 
}

print "ROOT\n====\n";
output($root->{'count'}, $root, "", 20);
print "LEAF\n====\n";
output($leaf->{'count'}, $leaf, "", 20);
