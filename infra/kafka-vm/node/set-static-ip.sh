#!/usr/bin/env bash
# Gives the lab NIC (the interface without a default route) a fixed IP via netplan,
# and rewrites the managed kafka-cluster block in /etc/hosts.
# Usage: set-static-ip.sh <ip/prefix> <hosts file with "ip hostname" lines>
set -euo pipefail

ADDR="$1"
HOSTS_SRC="$2"

# eth0 = Default Switch (DHCP, default route). The other NIC is the lab one.
uplink="$(ip -o -4 route show default | awk '{print $5}' | head -1)"
lab=""
for dev in /sys/class/net/*; do
  name="$(basename "$dev")"
  [[ "$name" =~ ^(eth|en) ]] || continue
  [ "$name" = "$uplink" ] && continue
  lab="$name"
done
if [ -z "$lab" ]; then
  echo "no lab NIC found (only $uplink) - run 02-lab-network.ps1 first" >&2
  exit 1
fi
mac="$(cat /sys/class/net/$lab/address)"

# Match by MAC: interface names can change, the MAC of the adapter does not
cat > /etc/netplan/60-kafka-lab.yaml <<EOF
network:
  version: 2
  ethernets:
    kafka-lab:
      match:
        macaddress: "$mac"
      set-name: "$lab"
      dhcp4: false
      addresses: ["$ADDR"]
EOF
chmod 600 /etc/netplan/60-kafka-lab.yaml
# Not "netplan apply": it re-runs DHCP on eth0 too, which changes the uplink IP and
# drops the multipass session. Reconfigure only the lab interface.
netplan generate
networkctl reload
networkctl reconfigure "$lab"
sleep 2
echo "$lab ($mac) -> $(ip -o -4 addr show dev "$lab" | awk '{print $4}')"

sed -i '/# BEGIN kafka-cluster/,/# END kafka-cluster/d' /etc/hosts
{
  echo '# BEGIN kafka-cluster'
  tr -d '\r' < "$HOSTS_SRC"
  echo '# END kafka-cluster'
} >> /etc/hosts
