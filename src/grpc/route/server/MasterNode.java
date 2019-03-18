package grpc.route.server;

public class MasterNode extends RouteServerImpl {

    // 1. save meta data of files (which partition on which slave)

    // 2. send heartbeat to slaves

    // 3. hashing the data ( given file (parts) onto the 3 nodes)

    // 4. replication of each part twice

    // 5. update meta-data when a slave goes down or come up

    // 6. take care of load balancing(data replication) when node goes up or down

    // 7. talk with both client and other slaves
}
