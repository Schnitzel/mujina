#!/bin/sh

set -eu

if pidof mujina-minerd >/dev/null 2>&1; then
    killall mujina-minerd
    sleep 1
fi

if pidof mujina-minerd >/dev/null 2>&1; then
    echo "mujina-minerd is still running" >&2
    exit 1
fi

echo "mujina-minerd stopped"