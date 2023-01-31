#!/bin/bash -eu

PATH=$PATH:$(cd "$(dirname $0)" ; pwd)

exit 0

if [ $# -lt 1 ]
then
    printf "Usage: %s [slug...]\n" $(basename $0) >&2
    exit 1
fi

#    grep hrefAnnotated | \

for SLUG in $* ; do
    echo ------------------------------------------------------------
    echo $SLUG
    echo ------------------------------------------------------------

    chk.py flowmsp-prod $SLUG | \
	while read slug loc img href url ; do
	    case $href in
		hrefAnnotated)
		    fix_thumbnail.py flowmsp-prod $slug $loc $img createdAnnotatedImage
		    fix_thumbnail.py flowmsp-prod $slug $loc $img createAnnotatedThumbnail
		    ;;
		hrefThumbnail)
		    fix_thumbnail.py flowmsp-prod $slug $loc $img createAnnotatedThumbnail
		    ;;
		*)
		    ;;
	    esac
	done
done

exit 0

