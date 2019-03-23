

docker stop $(docker ps -a -q)
echo "stopped all docker containers"
docker rm $(docker ps -a -q)
echo "removed all docker containers"

sudo rm /var/lib/dhcpd/*

echo "deleting dhcpd lease file"

docker run -d --net=host -v /etc/dhcp/:/etc/dhcp -v /var/lib/dhcpd:/var/lib/dhcpd --name dhcp vinodkatta3/dhcpd

ant server

