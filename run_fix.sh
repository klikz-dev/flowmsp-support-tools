#!/bin/bash -eu

PATH=$PATH:$(cd "$(dirname $0)" ; pwd)

if [ $# -lt 1 ]
then
    printf "Usage: %s [slug...]\n" $(basename $0) >&2
    exit 1
fi

#    grep hrefAnnotated | \
AWS_PROFILE=flowmsp-prod

for SLUG in $* ; do
    echo ------------------------------------------------------------
    echo $SLUG
    echo ------------------------------------------------------------

    chk.py $AWS_PROFILE $SLUG | \
	while read slug loc img href url ; do
	    case $href in
		href)
		    fix_thumbnail.py $AWS_PROFILE $slug $loc $img resize-image
		    ;;
		hrefAnnotated)
		    fix_thumbnail.py $AWS_PROFILE $slug $loc $img createdAnnotatedImage
		    fix_thumbnail.py $AWS_PROFILE $slug $loc $img createAnnotatedThumbnail
		    ;;
		hrefThumbnail)
		    fix_thumbnail.py $AWS_PROFILE $slug $loc $img createAnnotatedThumbnail
		    ;;
		annotationMetadata|annotationSVG)
		    fix_testurl.py $AWS_PROFILE $slug
		    ;;
		*)
		    ;;
	    esac
	done
done

exit 0

