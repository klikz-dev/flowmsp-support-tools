#!/usr/bin/env python3

import os
import sys
import argparse
from PIL import Image, ImageFile

def resize_image(filename, requested_height):
    im = Image.open(filename)

    print(im)
    print("width", im.width)
    print("height", im.height)

    new_height = min(im.height, requested_height)
    scale = requested_height / im.height
    new_width = round(scale * im.width)

    print("new_width", new_width)
    print("new_height", new_height)

    out = im.resize((new_width, new_height))

    name, ext = os.path.splitext(filename)

    new_name = "{name}-{height}{ext}".format(
        name=name, height=requested_height, ext=ext)

    print("new name", new_name)
    out.save(new_name)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="resize images")
    parser.add_argument("images", nargs="+")
    args = parser.parse_args()

    for i, f in enumerate(args.images):
        resize_image(f, 1024)
        resize_image(f, 300)
