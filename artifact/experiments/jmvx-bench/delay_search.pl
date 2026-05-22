#!/usr/bin/perl
=pod

Usage: ./delay_search.pl [datasize mb] [buffersize kb] [max delay ms (not checked! add 1)]

ReadWriteMicrobenchmark copies data from /dev/zero to /dev/null
It simulates cpu usage via a spin loop which delays IO for a set interval of time.

Goal: to find out how long of a delay causes the recorder to outperform the leader
Solution: Binary search over a range of values we are willing to check

We have 3 parameters for the read write benchmark:
data size, buffer size, and delay.
Going to fix data and buffer size.
For each fixed setting, this script will specify a range of delay values I'm willing to test
To reduce the time spent running the benchmark, we'll use a binary search to identify which value in the interval actually actually sees the transition.

=cut

#so future onlookers don't kill me
use strict;
use warnings;

=pod
Parses the runtime(s) of the benchmark
and returns the average.
=cut
#maybe should use a list ref?
sub get_avg{
  my @output = @_;
  my $n = 0;
  my $sum = 0;
  #filter to line which report the duration
  foreach(grep(/Duration/, @output)){
    /(\d+)/; #parse the number
    $sum += $1;
    $n += 1;
  }
  return 0 if $n == 0;
  return $sum / $n; #compute avg
}

=pod
Runs a hybrid MVX/Recorder
Relies on the bash script run-readwrite-mvx.sh,
which will in turn run several iterations for us.
Returns the average execution time
=cut
sub mvx {
  my ($datsize, $bufsize, $delay) = @_;
  my @output = `./run-readwrite-mvx.sh $datsize $bufsize $delay`;
  return get_avg(@output);
}


=pod
Runs the recorder.
Relies on the bash script run-readwrite-rec.sh, which will in turn run several iterations for us.
Returns the average execution time
=cut
sub rec {
  my ($datsize, $bufsize, $delay) = @_;
  my @output = `./run-readwrite-rec.sh $datsize $bufsize $delay`;
  return get_avg(@output);
}

my $datsize = shift @ARGV; #1024;
my $bufsize = shift @ARGV; #512;

my $quiet = 0; #bool flag, set to 0 for debug info

my $lo = 0; #inclusive, can be checked
my $hi = shift @ARGV; #8; #exclusive! Prior iter may have checked it, but it won't be checked again.

my $mvxtime = 0;
my $rrtime = 0;

#last value we saw that satisfied rrtime < mvxtime
my $pivot = -1;

#while we have values left to check
#only way lo = hi is if we've checked it
while($lo != $hi){
  my $half = int(($hi + $lo) / 2);
  print("Interval: [$lo, $hi), Half: $half\n") unless $quiet;
  $mvxtime = mvx($datsize, $bufsize, $half);
  $rrtime  = rec($datsize, $bufsize, $half); 

  print("MVX: $mvxtime, REC: $rrtime\n") unless $quiet;

  #adjust the interval based on the outcome
  #numerical pericision concern? should we check $rrtime - $mvxtime < 0
  if($rrtime < $mvxtime) {
    $pivot = $half;
    $hi = $half;   
    print("Pivot: $pivot\n") unless $quiet;
  }else{
    $lo = $half + 1;
  }
}


print("$datsize, $bufsize, $pivot\n");
