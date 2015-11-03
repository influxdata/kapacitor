#!/bin/sh

rm -f /etc/default/kapacitor

# Systemd
if which systemctl > /dev/null 2>&1 ; then
    systemctl disable kapacitor
    rm -f /lib/systemd/system/kapacitor.service
# Sysv
else
    if which update-rc.d > /dev/null 2>&1 ; then
        update-rc.d -f kapacitor remove
    else
        chkconfig --del kapacitor
    fi
    rm -f /etc/init.d/kapacitor
fi
