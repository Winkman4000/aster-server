#!/bin/bash
# First-boot setup for the aster-server VM (Debian 12, e2-micro): Python, a swap file
# (the VM has 1 GB of RAM), and a service that runs server.py --poll. The code goes in
# /opt/aster and the runner's URL and token in /etc/aster.env (both copied in by Aster's
# setup over SSH; nothing secret is in this script).
set -e

if [ ! -f /swapfile ]; then
  fallocate -l 1G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

apt-get update -q
apt-get install -y -q python3 tzdata

id aster >/dev/null 2>&1 || useradd --system --create-home --shell /usr/sbin/nologin aster
mkdir -p /opt/aster
chown aster:aster /opt/aster

cat > /etc/systemd/system/aster.service <<'UNIT'
[Unit]
Description=aster-server: runs Aster notebooks for aster-runner
After=network-online.target
Wants=network-online.target

[Service]
User=aster
WorkingDirectory=/opt/aster
EnvironmentFile=/etc/aster.env
# The VM has IPv6 only; wait for its route (it arrives a little after boot).
ExecStartPre=/bin/sh -c 'for i in $(seq 1 60); do ip -6 route show default | grep -q . && exit 0; sleep 2; done'
ExecStart=/usr/bin/python3 -u /opt/aster/server.py --poll
Restart=always
RestartSec=5
# Notebooks are code: keep them away from the rest of the system.
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/aster /tmp
PrivateTmp=true
MemoryMax=800M

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable aster.service
