#!/bin/sh
###############################################################################
# This script is used to provide the password for an encrypted file system by #
# launching a minimal web server. It is meant to replace /sbin/cryptsetup for #
# an initramfs. The original cryptsetup should be moved to /sbin/cs instead.  #
#                                                                             #
###############################################################################
#                                                                             #
# Copyright (c) 2011 Henrik Gulbrandsen <henrik@gulbra.net>                   #
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

. /scripts/functions

### Variables reserved for this script ########################################

cs_priv="/etc/ssl/private/boot.key"
cs_cert="/etc/ssl/certs/boot.crt"
cs_host="boot.example.com"
cs_home="/bozo"

cs_fifo="$cs_home/tmp/fifo"
cs_open=false

### Web Server Handling #######################################################

cs_startWebServer() {
    echo "Removing stale GIFs and locks"
    rm -f "$cs_home"/data/hiding[0-9]*.gif
    rm -f "$cs_home/tmp/lock"

    echo "Starting web server"
    bozohttpd -b -s -S "Introdus 1.0" -U www-data -t $cs_home -n -c /cgi-bin \
        -Z $cs_cert $cs_priv data $cs_host

    # Remember the start
    cs_open=true
}

cs_stopWebServer() {
    echo "Stopping web server"
    killall bozohttpd
    cs_open=false
}

### Password Handling #########################################################

fetch_password() {
    # Prepare the CGI script...
    if ! $cs_open; then
        modprobe af_packet
        IPOPTS=dhcp; DEVICE=eth0
        configure_networking;
        cs_startWebServer;
    fi

    # ...and wait for its data
    echo "Waiting for password"
    password=$(cat $cs_fifo)
    echo "Got password"
}

accept_password() {
    echo "Accepting password"
    echo "OK" > $cs_fifo;
    sleep 1; cs_stopWebServer;
}

reject_password() {
    echo "Rejecting password"
    echo "BAD" > $cs_fifo;
}

### Core Script ###############################################################

if [ "_$1" = _isLuks ]; then
    exec /sbin/cs "$@"
fi

echo "Replaced cryptsetup"

while fetch_password; do
    if printf "%s" "$password" | /sbin/cs "$@"; then
        accept_password; break;
    else
        reject_password;
    fi
done

###############################################################################
