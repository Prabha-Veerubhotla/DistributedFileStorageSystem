# Project Fluffy

### Description
Project Fluffy is a distributed system that acts a data-storage platform where network of server nodes are connected with switches and LAN cables.  
Various distributed systems concepts are used to make Project Fluffy fault tolerant and highly scalable.
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

### Steps to run the program on the super node:
1. Build and Run the DHCP server.
   Details:
   * clone the dhcp-server from the github link provided in software requirements.(A docker file will be present in the folder.)
   * Build docker image using docker file. COMMAND: *"docker build -t <username>/dhcpd ."*
   * Install "isc-dhcp-server" package in ubuntu. COMMAND: *"sudo apt-get install isc-dhcp-server"* (This is for Ubuntu, not tested in MAC or Windows).
   * Now, you should be able to locate /etc/dhcp/dhcpd.conf file. Add following text in dhcpd.conf file:
   ``` 
        subnet 10.250.10.0  netmask 255.255.255.0 {
          option routers                  10.250.10.254;
          option subnet-mask              255.255.255.0;
          option domain-search              "example.com";
          option domain-name-servers       10.250.10.1;
          option time-offset              -18000;     # Eastern Standard Time
          range 10.250.10.80 10.250.10.100;
        } 
   ```
   * This allows dhcp server to assign the range of IPs specified here to allocate to the new nodes joining in the network.
   * Now, run the docker image to start the dhcp server from the dhcp-server folder you cloned previously. 
     - COMMAND: *"docker run --net=host -v /etc/dhcp/:/etc/dhcp -v /var/lib/dhcpd:/var/lib/dhcpd --name dhcp <username>/dhcpd"* 
   * Now DHCP server should be up and running.
   * [More on DHCP](https://en.wikipedia.org/wiki/Dynamic_Host_Configuration_Protocol)
      
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
