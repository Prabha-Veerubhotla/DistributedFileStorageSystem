### Build and Run the DHCP server.
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
