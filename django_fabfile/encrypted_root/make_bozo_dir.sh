#!/bin/sh
###############################################################################
# This script prepares the bozo directory for a minimal activation webserver. #
#                                                                             #
# See this thread for some background information:                            #
#   http://developer.amazonwebservices.com/connect/thread.jspa?threadID=24091 #
#                                                                             #
###############################################################################
#                                                                             #
# Copyright (c) 2009, 2011 Henrik Gulbrandsen <henrik@gulbra.net>             #
#                                                                             #
# This software is provided 'as-is', without any express or implied warranty. #
# In no event will the authors be held liable for any damages arising from    #
# the use of this software.                                                   #
#                                                                             #
# Permission is granted to anyone to use this software for any purpose,       #
# including commercial applications, and to alter it and redistribute it      #
# freely, subject to the following restrictions:                              #
#                                                                             #
# 1. The origin of this software must not be misrepresented; you must not     #
#    claim that you wrote the original software. If you use this software     #
#    in a product, an acknowledgment in the product documentation would be    #
#    appreciated but is not required.                                         #
#                                                                             #
# 2. Altered source versions must be plainly marked as such, and must not be  #
#    misrepresented as being the original software.                           #
#                                                                             #
# 3. This notice may not be removed or altered from any source distribution,  #
#    except that more "Copyright (c)" lines may be added to already existing  #
#    "Copyright (c)" lines if you have modified the software and wish to make #
#    your changes available under the same license as the original software.  #
#                                                                             #
###############################################################################

set -e

# Check the arguments
if [ -z "$1" ]; then
    echo "${0##*/} <bozo-dir>"
    exit 1
fi

# Directories
home=$(dirname $0)
bozo=$1

# Create the directory hierarchy
DIRECTORIES="bin cgi-bin data dev lib tmp"
for dir in $DIRECTORIES; do
    mkdir -p "$bozo/$dir"
done

# Create the needed special files
mknod -m 666 $bozo/dev/null c 1 3
mkfifo $bozo/tmp/fifo

# Copy needed programs and libraries
PROGRAMS="cat cp dd ln printf rm sed sh tr"
for file in $PROGRAMS; do
    path=$(which $file)
    echo "Copying $path"
    cp "$path" "$bozo/bin/$file"
    for lib in $(ldd $path | sed 's/\t//; s/.*=> //; s/ (.*//'); do
        if [ "${lib#linux-gate.so}" != "$lib" ]; then continue; fi
        if [ -e "$bozo/$lib" ]; then continue; fi
        echo "Copying $lib"
        cp --parents "$lib" "$bozo"
    done
done

# Copy the web files
cp "${home}/activate.cgi" "$bozo/cgi-bin/"
cp "${home}/index.html" "$bozo/data/"
cp "${home}/hiding.gif" "$bozo/data/"

# Give full access to www-data
chown -R www-data:www-data "$bozo"

###############################################################################
