#!/bin/sh

set -eu

RUST_LOG=debug \
MUJINA_CONFIG=/home/root/mujina-hb2.toml \
MUJINA_POOL_URL='stratum+tcp://pool.256foundation.org:3333' \
MUJINA_POOL_USER='npub1ql2zzp3g6yndgz05js7wdc4qkr88wkyne5nw2cc7csrtzqs0yeesgwrxya.mujina-jPro-amlogic' \
MUJINA_API_LISTEN='0.0.0.0:7785' \
/home/root/mujina-minerd