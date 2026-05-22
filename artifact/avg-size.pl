#!/usr/bin/perl 
=pod
Computes the average size of files provides directories
This does not recurse through the tree
In other words, it only looks at the first level
=cut

use feature qw( say );
for(@ARGV){ 
  #black magic, use du to compute the sizes in mb
  #use ls | wc to figure out how many files were present
  #I could do this all in perl, but this was the original 1 liner
  say $_ . " " . (`du -m $_` / `ls $_ | wc -l`) . " MB"; 
}

