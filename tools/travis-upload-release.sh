#!/bin/bash

echo upload-release $SPEC
if [ "$SPEC" = "embedded" ]; then
    tar cvzf $TARBALL_NAME -C rel/mongooseim/ .
    curl --ftp-create-dirs -T $TARBALL_NAME -u $FTP_USER:$FTP_PASSWORD ftp://$FTP_SERVER/mongooseim/$TARBALL_NAME
    echo $TARBALL_NAME uploaded
fi
