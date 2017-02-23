#!/bin/bash

function disable_systemd {
    systemctl disable kapacitor
    rm -f /lib/systemd/system/kapacitor.service
}

function disable_update_rcd {
    update-rc.d kapacitor remove
    rm -f /etc/init.d/kapacitor
}

function disable_chkconfig {
    chkconfig --del kapacitor
    rm -f /etc/init.d/kapacitor
}

if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if [[ "$1" = "0" ]]; then
        # Kapacitor is no longer installed, remove from init system
        rm -f /etc/default/kapacitor

        if [[ "$(readlink /proc/1/exe)" == */systemd ]]; then
            disable_systemd
        else
            # Assuming sysv
            disable_chkconfig
        fi
    fi
elif [[ -f /etc/debian_version ]]; then
    # Debian/Ubuntu logic
    if [[ "$1" != "upgrade" ]]; then
        # Remove/purge
        rm -f /etc/default/kapacitor

        if [[ "$(readlink /proc/1/exe)" == */systemd ]]; then
            disable_systemd
        else
            # Assuming sysv
            disable_update_rcd
        fi
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
        # Amazon Linux logic
        if [[ "$1" = "0" ]]; then
            # Kapacitor is no longer installed, remove from init system
            rm -f /etc/default/kapacitor
            disable_chkconfig
        fi
    fi
fi
