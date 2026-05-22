#!/usr/bin/perl
use List::Util qw(any all sum);

# read in counts as an array of hash refs {event, count}
# an array instead of a map because the events are already sorted
# with respect to count.
@counts = ();
for(<STDIN>){
  if( /(.*)=(\d*)/ ) {
    push @counts, {event => $1, count => $2};
  }
}

# lists of compiled regexes, one to look for matches
@matchers = ();
# and one to look for negations (failed matches)
@negators = ();

# pull in key filters from argument list
for my $arg (@ARGV) {
  # precompile the regexes
  push @matchers, qr/$1/ if $arg =~ /match=(\S*)/;
  push @negators, qr/$1/ if $arg =~ /negate=(\S*)/;
}

# keep keys that match at least 1 regex
@counts = grep( { 
  my $entry = $_; 
  any { ${$entry}{event} =~ m/$_/ } @matchers
} @counts) if @matchers > 0;

# keep keys that fail to match all negator regexes
@counts = grep( { 
  my $entry = $_; 
  all { ${$entry}{event} !~ m/$_/ } @negators
} @counts) if @negators > 0;

# compute the sum of events remaining
$total = sum( map { ${$_}{count} } @counts);

# print results as event=count <tab>  proportion occurred
for(@counts){
  $c = ${$_}{count};
  $per = $c / $total;
  print "${$_}{event}=$c\t$per\n";
}

print "Total=$total\n"

