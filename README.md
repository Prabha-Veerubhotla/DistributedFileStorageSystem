# Project Fluffy

### Software requirements:
1. Java 1.8
2. Apache Ant
   [Ant installation steps](https://ant.apache.org/manual/install.html)
3. Protobuf compiler
   [Protobuf installation](https://github.com/protocolbuffers/protobuf)
4. GRPC
   Build GRPC **codegen java_plugin** from [here](https://github.com/grpc/grpc-java/tree/master/compiler)
5. DHCP server
   Requirements: **Docker**
   [DHCP installation](https://github.com/container-images/dhcp-server)
6. Redis - Used as In-Memory database
7. MongoDB - Used for back-up.

### Steps to run Project Fluffy:
1. 
      
2. Compile .proto file in the resourses folder using protoc.Use build_pb.sh in the project folder to compile the .proto file.
   - COMMAND: *".build_pb.sh"*
3. Build the server program using Ant using the build.xml in the project folder to build the the program. 
   - COMMAND: *"ant build"*
4. Run the server using startServer.sh file in project folder.
   - COMMAND: *"./startServer.sh conf/server.conf"*

### On the other nodes:
1. Set the wired connection IPv4 settings to DHCP.
2. And follow the steps 2,3,4 as in for super node.
3. **Important!!**
   Start the server before connecting to the network. 
   **Reason:** As soon as a node connects to the super node, DHCP server assigns an IP to it and updates /etc/dhcp/dhcpd.leases.Then immediately, 
   * Java thread running in the GRPC server reads the ip address from the dhcpd.leases and try to send hello msg to the newly connected node.
   * If the server isn't started then the message sent from the super node will be lost.

### Demo1 description:
This demo successfully detects new nodes in the network and sends a hello msg to them.

### Implementation:
1. This demo uses grpc to communicate with other nodes in the network. Every node has to run grpc server before entering the network.
2. A super node in the network (which also runs grpc server), run DHCP server to allocate IPs to newly joined nodes and sends hello message to them. Also, sends these IPs to the nodes already present in the network.

### Technical details:
#### On the super-node:
1. DHCP server writes IPs it allocated/released/reallocated to dhcpd.leases file in the /var/lib/dhcpd/dhcpd.leases.
2. The server has a java thread that monitors the dhcpd.leases for any modification. Acheived this by scheduling a timer task to monitor dhcpd.leases file every second.
3. If this task detects file modification then we read the dhcpd.leases file for the IPs using shell script(*fetch_ip.sh*) and compare them with the  old IP list.
4. If we find any new IP then we build a channel for the new IP and send hello msg. Also, sends this new IP address to all the nodes. 
