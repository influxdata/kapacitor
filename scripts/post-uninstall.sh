#!/bin/bash

function uninstall_init {
    rm -f /etc/init.d/kapacitor
}

function uninstall_systemd {
    rm -f /lib/systemd/system/kapacitor.service
}

function disable_systemd {
    systemctl disable kapacitor
}

function disable_update_rcd {
    update-rc.d -f kapacitor remove
}

function disable_chkconfig {
    chkconfig --del kapacitor
}

if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if [[ "$1" = "0" ]]; then
        # Kapacitor is no longer installed, remove from init system
        rm -f /etc/default/kapacitor

        if [[ "$(readlink /proc/1/exe)" == */systemd ]]; then
            disable_systemd
            uninstall_systemd
        else
            # Assuming SysV
            # Run update-rc.d or fallback to chkconfig if not available
            if which update-rc.d &>/dev/null; then
                disable_update_rcd
            else
                disable_chkconfig
            fi
            uninstall_init
        fi
    fi
elif [[ -f /etc/debian_version ]]; then
    # Debian/Ubuntu logic
    if [[ "$1" != "upgrade" ]]; then
        # Remove/purge
        rm -f /etc/default/kapacitor

        if [[ "$(readlink /proc/1/exe)" == */systemd ]]; then
            disable_systemd
            uninstall_systemd
        else
            # Assuming SysV
            # Run update-rc.d or fallback to chkconfig if not available
            if which update-rc.d &>/dev/null; then
                disable_update_rcd
            else
                disable_chkconfig
            fi
            uninstall_init
        fi
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
        # Amazon Linux logic
        if [[ "$1" = "0" ]]; then
            # Kapacitor is no longer installed, remove from init system
            rm -f /etc/default/kapacitor

            # Run update-rc.d or fallback to chkconfig if not available
            if which update-rc.d &>/dev/null; then
                disable_update_rcd
            else
                disable_chkconfig
            fi
            uninstall_init
        fi
    fi
fi
