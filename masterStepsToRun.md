### Steps to run master node
1. Build and Run the DHCP server.
   Details:
   * clone the dhcp-server from the github link provided in software requirements.(A docker file will be present in the folder.)
   * Build docker image using docker file. 
      -`docker build -t <username>/dhcpd .`
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
3. Run redis on a terminal using the command **redis-server**
4. By default DHCP is disabled. To enable DHCP change arg value of target master to yes and then build.
5. Run the master using build.xml file in project folder.
   - COMMAND: *"ant master"*
