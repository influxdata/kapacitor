#!/bin/sh
BIN_DIR=/usr/bin
DATA_DIR=/var/lib/kapacitor
LOG_DIR=/var/log/kapacitor
SCRIPT_DIR=/usr/lib/kapacitor/scripts

if ! id kapacitor >/dev/null 2>&1; then
        useradd --system -U -M kapacitor -s /bin/false -d $DATA_DIR
fi
chmod a+rX $BIN_DIR/kapacitor*

mkdir -p $LOG_DIR
chown -R -L kapacitor:kapacitor $LOG_DIR
mkdir -p $DATA_DIR
chown -R -L kapacitor:kapacitor $DATA_DIR

test -f /etc/default/kapacitor || touch /etc/default/kapacitor

# Systemd
if which systemctl > /dev/null 2>&1 ; then
    cp -f $SCRIPT_DIR/kapacitor.service /lib/systemd/system/kapacitor.service
    systemctl enable kapacitor

# Sysv
else
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/kapacitor
    chmod +x /etc/init.d/kapacitor
    if which update-rc.d > /dev/null 2>&1 ; then
        update-rc.d -f kapacitor remove
        update-rc.d kapacitor defaults
    else
        chkconfig --add kapacitor
    fi
fi
