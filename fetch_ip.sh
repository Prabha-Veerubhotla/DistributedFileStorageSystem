#Shell command to find ip addresses
grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' /var/lib/dhcpd/dhcpd.leases
