#!/usr/bin/perl 
=pod
Usage: ./overheads.pl vanilla_summary.txt role_summary.txt

Computes overhead between a table of vanilla results and some other runtime
Assumes each table is formatted as:

Benchmark | # Passed | Harness | VM
name      | #        | w +- x  | y +- z

This script attempts to iterate past chunk to find the table
So notes won't screw it up.
However, THIS SCRIPT ASSUMES THE ROWS ARE MAPPED 1 TO 1.
In other words, THE BENCHMARKS BETTER MATCH (IN NUMBER AND ORDER).
=cut

format STDOUT =
@<<<<<<<<< |  @##.###x | @##.###x
$shortname,  $harn_over, $vm_over
.

print "Benchmark  |  Harness  |   VM\n";

if (@ARGV != 2){
  print "Usage: ./overheads.pl vanilla_summary.txt role_summary.txt\n";
  exit 1;
}

my ($basefile, $otherfile) = @ARGV;

open(my $basefd , "<", $basefile)  or die "Could not open $basefile";
open(my $otherfd, "<", $otherfile) or die "Could not open $otherfile";

#my $sep = "\|";

sub likely_row {
  return @_ == 4;
}

sub get_fields {
  local $s = shift;
  return split /\|/, $s;
}

sub parse_to_data {
  local $fd = shift;
  while(<$fd>){
     local @fields = get_fields $_;
     next unless likely_row @fields;
     return; #read header!
  }
}

my $name = 0;
my $count = 1;
my $harness = 2;
my $vm = 3;

sub extract_time {
  local $str = shift;
  local @data = split /\s*\+-\s*/, $str;
  local $dat = $data[0];
  #strip whitespace
  $dat =~ s/^\s+//;
  $dat =~ s/\s+$//;
  return $dat;
}

sub avg {
  local @numbers = @_;
  #compute average
  local $sum = 0;
  local $i = @numbers;

  #sentinel
  if($i == 0){
    return -1;
  }
  
  for(@numbers){
    $sum += $_;
  }
  local $avg = $sum / $i;
  return $avg; 
}

parse_to_data $basefd;
parse_to_data $otherfd;

my @harn_vals = ();
my @vm_vals   = ();

while(1){
  my $baseline = <$basefd>;
  my $otherline = <$otherfd>;
  my @base  = get_fields $baseline;
  my @other = get_fields $otherline;

  last unless (likely_row @base and likely_row @other);
  if ($base[$name] != $other[$name]){
    print "Benchmarks are out of order, stopping...\n";
    exit 1;
  }

  my $base_harness = extract_time @base[$harness];
  my $base_vm      = extract_time @base[$vm];
 
  my $other_harness = extract_time @other[$harness];
  my $other_vm      = extract_time @other[$vm];

  $shortname = $base[$name];
  if ($base_harness == 0) {
    $harn_over = -1; #sentinel
  }else{
    $harn_over = $other_harness / $base_harness;
  }
  
  if ($base_vm == 0) {
    $vm_over = -1; #sentinel
  }else{
    $vm_over   = $other_vm      / $base_vm;
  }
  write;

  push(@harn_vals, $harn_over);
  push(@vm_vals,   $vm_over);
}

$shortname = "AVG";
$harn_over = avg @harn_vals;
$vm_over   = avg @vm_vals;
write;
