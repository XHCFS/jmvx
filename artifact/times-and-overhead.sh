#!/bin/sh

VANILLA=""
PREPEND=""

while getopts 'p:v:' opt; do
    case "$opt" in
        p) PREPEND="${OPTARG}-"
        ;;
        v) VANILLA="$OPTARG"
        ;;
        *) exit;
    esac
done

shift $(( $OPTIND - 1))

FILES="$*"

if [ "$VANILLA" = "" ]; then
    echo "Vanilla summary file required. Specify with -v [file]"
    exit;
fi

SUMMARY=${PREPEND}summary.txt
OVERHEAD=${PREPEND}overhead.txt

$ROOT/times.pl $FILES | tee $SUMMARY
echo "Saved $SUMMARY"
$ROOT/overheads.pl $VANILLA $SUMMARY | tee $OVERHEAD
echo "Saved $OVERHEAD"
