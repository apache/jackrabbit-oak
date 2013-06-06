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
  my ($total, $info, $prefix) = @_;

  my @children = values %{$info->{'children'}};
  @children = grep { $_->{'count'} * 50 > $total } @children or return;
  @children = sort { $b->{'count'} - $a->{'count'} } @children;

  my $last = $children[-1];
  for my $child (@children) {
    printf "%s+%d%% (%d%%) %s\n",
      $prefix,
      $child->{'count'} * 100 / $total,
      $child->{'count'} * 100 / $info->{'count'},
      $child->{'frame'};
    output($total, $child, ($child == $last) ? "$prefix " : "$prefix|");
  }
}

my $trace = '';
while (<>) {
  /^TRACE (\d+)/  and $trace = $1 and $traces{$trace} = [] and next;
  /^\s+(?:org\.apache\.jackrabbit\.)?([a-z].*?)(\(.*)?\r?\n/ and $trace and $traces{$trace} = [ $1, @{$traces{$trace}} ] and next;
  /^rank/ and $trace and $trace = '' and next;
  /^\s*\d+\s+\S+%\s+\S+%\s+(\d+)\s+(\d+)/ and accumulate($2, $1); 
}

print "ROOT\n====\n";
output($root->{'count'}, $root, "");
print "LEAF\n====\n";
output($leaf->{'count'}, $leaf, "");
