#!/bin/sh
###############################################################################
# This CGI script is used to get the password for an encrypted file system.   #
# It should be installed together with other files to form a web interface.   #
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

PATH=$PATH:/bin
IFS='&';

# Some default values
type="password"     # Type of the text field ("password" or "text")
darkness="hidden"   # Set to "visible" to darken status text after 3 seconds
image="/hiding.gif" # An animated gif representing the darkness...
color="gray"        # The color of the status text
disabled=""         # Set to "disabled" to disable the text field
status=""           # Status text displayed below the text field
new_key=""          # New one-time passphrase sent after OK in the response
key=""              # The passphrase that is returned to the pre_init.sh script

### The main script ###########################################################

# A default value is nice to have
if [ -z "$CONTENT_LENGTH" ]; then
    CONTENT_LENGTH="0"
fi

# Fetch the data if CONTENT_LENGTH consists entirely of digits
if [ -z "$(printf "$CONTENT_LENGTH" | tr -d '[0-9]')" ]; then
  data=$(dd bs=1 count="$CONTENT_LENGTH" 2> /dev/null)
fi

# Assign all relevant variables from the data
for assignment in $data; do

  # Use black magic to replace '+' and %XX codes with the original characters
  pattern='\\%\\\([0-9A-Fa-f]\)\\\([0-9A-Fa-f]\)'
  command='$(printf \\\\$(printf "%03o" 0x\1\2))'
  script='s/+/ /g; s/\(.\)/\\\1/g;'" s/$pattern/$command/g"
  text=$(echo "$assignment" | sed "$script")
  assignment=$(IFS= eval echo $text)

  # Make the right assignment
  case $assignment in
      key=*) key=${assignment#key=};;
  esac
done

# Only a single instance of this script is allowed
if ln -s activate.cgi /tmp/lock 2> /dev/null; then
    trap "rm -f /tmp/lock" EXIT
else
    status="Job Collision"
    darkness="visible"
    color="orange"
    key=""
fi

# If a password was given:
if [ -n "$key" ]; then

    # Hand it to the init script and wait for a response
    printf "$key" > /tmp/fifo
    response=$(cat /tmp/fifo)
    new_key=${response#OK:}

    # Try to extract a new password from the response
    if [ "$new_key" != "$response" ]; then
        response="OK"
    else
        new_key=""
    fi

    # Make suitable changes to the result page
    if [ "$response" = "OK" ]; then
        disabled="disabled"
        status="Authorized"
        color="green"
        type="text"
    else
        status="Access Denied"
        darkness="visible"
        color="red"
    fi
fi

# A new GIF each time, since Firefox won't restart the animation
if [ "$darkness" = "visible" ]; then

    # Select an unused image name...
    index=1
    while [ -e "/data/hiding${index}.gif" ]; do
        index=$((index+1))
    done

    # ...and copy the original image
    image=/hiding${index}.gif
    cp /data/hiding.gif /data${image}
fi

### The web-page template #####################################################

cat <<EOF
Content-type: text/html

<html>
  <head>
    <title>Activation</title>
    <style type="text/css">
      body {
        background: black;
      }
      #box {
        position: absolute;
        left: 50%; top: 40%;
        width: 320px; margin-left: -160px;
        height: 80px; margin-top: -40px;
        border: 1px solid gray;
        font: 16px sans-serif;
        text-align: center;
        color: gray;
      }
      .text {
        left: 50%; top: 25%;
      }
      .field {
        left: 50%; top: 50%;
        padding-left: 6px;
        padding-top: 2px;
      }
      .hiding {
        background: url("$image");
        visibility: $darkness;
        left: 50%; top: 80%;
      }
      .status {
        left: 50%; top: 80%;
        font-weight: bold;
        color: $color;
      }
      .line {
        position: absolute; border: none;
        width: 280px; margin-left: -140px;
        height: 20px; margin-top: -10px;
      }
    </style>
  </head>
  <body>
    <form action="activate.cgi" method="post">
      <div id="box">
        <div class="line text">Enter the password</div>
        <input class="line field" name="key" $disabled
          type="$type" value="$new_key">
        <div class="line status">$status</div>
        <div class="line hiding"></div>
      </div>
    </form>
  </body>
</html>
EOF

###############################################################################
