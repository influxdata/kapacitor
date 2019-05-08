#!/bin/bash

BIN_DIR=/usr/bin
DATA_DIR=/var/lib/kapacitor
LOG_DIR=/var/log/kapacitor
SCRIPT_DIR=/usr/lib/kapacitor/scripts

function install_init {
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/kapacitor
    chmod +x /etc/init.d/kapacitor
}

function install_systemd {
    cp -f $SCRIPT_DIR/kapacitor.service /lib/systemd/system/kapacitor.service
}

function enable_systemd {
    systemctl enable kapacitor
}

function enable_update_rcd {
    update-rc.d kapacitor defaults
}

function enable_chkconfig {
    chkconfig --add kapacitor
}

chmod a+rX $BIN_DIR/kapacitor*

test -f /etc/default/kapacitor || touch /etc/default/kapacitor

# Distribution-specific logic
if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if [[ "$(readlink /proc/1/exe)" == */systemd ]]; then
        install_systemd
        # Do not enable service
    else
        # Assuming SysV
        install_init
        # Do not enable service
    fi
elif [[ -f /etc/debian_version ]]; then
    # Debian/Ubuntu logic

    # Ownership for RH-based platforms is set in build.py via the `rmp-attr` option.
    # We perform ownership change only for Debian-based systems.
    # Moving these lines out of this if statement would make `rmp -V` fail after installation.
    test -d $LOG_DIR || mkdir -p $LOG_DIR
    test -d $DATA_DIR || mkdir -p $DATA_DIR
    chown -R -L kapacitor:kapacitor $LOG_DIR
    chown -R -L kapacitor:kapacitor $DATA_DIR
    chmod 755 $LOG_DIR
    chmod 755 $DATA_DIR

    if [[ "$(readlink /proc/1/exe)" == */systemd ]]; then
        install_systemd
        enable_systemd
    else
        # Assuming SysV
        install_init
        # Run update-rc.d or fallback to chkconfig if not available
        if which update-rc.d &>/dev/null; then
            enable_update_rcd
        else
            enable_chkconfig
        fi
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
        # Amazon Linux logic
        install_init
        # Do not enable service
    fi
fi
