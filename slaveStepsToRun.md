### Steps to run the slave nodes
1. Set the wired connection IPv4 settings to DHCP.
2. Run redis in a terminal using command **redis-server**
3. Run mongoDB in a terminal using command **mongod**
4. Edit the conf/slave.conf file to choose the port number for the gRPC server.
4. In the project home folder path, run command **ant slave**
5. **Important!!**
   Start the server before connecting to the network. 
   **Reason:** As soon as a node connects to the super node, DHCP server assigns an IP to it and updates /etc/dhcp/dhcpd.leases.Then immediately, 
   * Java thread running in the GRPC server reads the ip address from the dhcpd.leases and try to send hello msg to the newly connected node.
   * If the server isn't started then the message sent from the super node will be lost.