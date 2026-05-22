#!/usr/bin/perl 
=pod
Script to compute the average runtime and standard deviation of program runs
Usage ./times file0.txt file1.txt ... fileN.txt

The program has some extra logic to shorten the table name entry for JMVX
See the `get_shortname` function to adjust.
=cut

#output format of the table
#fields are set in each loop iteration
format STDOUT =
@<<<<<<<<< | @######  | @######.### +- @######.### | @######.### +- @####.###
$shortname,  $passing,  $avg_harn,     $dev_harn,    $avg_vm,       $dev_vm
.
print
"Benchmark  | # Passed | Harness Runtime (msec)     | VM Runtime (msec)\n";

#gets the shortname of the benchmark to use for the table
#change this function if you care to change the table rendering
sub get_shortname {
  $_ = shift;
  return $1 if /\w*-?(\w+)\.txt/;
  return $_;
}

#returns avg and standard deviation of a list of numbers
sub summarize {
  local @numbers = @_;
  #compute average
  local $sum = 0;
  local $i = @numbers;

  #sentinel
  if($i == 0){
    return -1, -1;
  }
  
  for(@numbers){
    $sum += $_;
  }
  local $avg = $sum / $i;
  
  #compute standard deviation
  local $acc = 0;
  for(@numbers){
    local $x = $_ - $avg;
    $acc += $x * $x;
  }
  
  local $stddev = 0; #no deviation of 1 number
  if($i > 1){
    $stddev = sqrt($acc / ($i - 1));
  }
  return $avg, $stddev
}

for $filename (@ARGV){
  unless(open($fd, "<", $filename)){
    print "Failed to open $filename";
    next;
  }
 
  $shortname = get_shortname $filename;
  
  #initialize data for avg +- stddev calc
  local @numbers_bench = ();
  local @numbers_vm    = ();
  local $passed = 0;
  $passing = 0; #not local, used in table
  while(<$fd>){
    if(/PASSED in (\d+)/){
      #bach/chopin benchmark time
      push(@numbers_bench, $1);
      $passed = 1;
      $passing++;
    }elsif(/Duration: (\d+)/){
      #micro benchmark time
      push(@numbers_bench, $1);
      $passed = 1;
      $passing++;
    }elsif(/copied, ([\.\d]+) s/){
      #dd reported time
      local $t = $1 * 1000; #convert to msec
      push(@numbers_bench, $t);
      $passed = 1;
      $passing++;
    }elsif(/real\s+([\.\d]+)/ and $passed) { 
      #value from <time> command
      local $t = $1 * 1000;
      #print "$t\n";
      push(@numbers_vm, $t);
      $passed = 0;
    }elsif(/(\d+):([\.\d]+)elapsed/ and $passed){
      #non posix compliant value from <time> command
      local $t = ((60 * $1) + $2) * 1000;
      #print "$t\n";
      push(@numbers_vm, $t);
      $passed = 0;
    }
  }

  ($avg_harn, $dev_harn) = summarize @numbers_bench;
  ($avg_vm  , $dev_vm)   = summarize @numbers_vm;
  
  #write the formmated output
  write;
}
