package grpc.route.server;

public class SlaveNode extends RouteServerImpl {

    // 1. put data into the database
    public static boolean putFilePartition(String filename) {

        return true;
    }


    // 2. reqtrieve data from the database and send it to server upon a request

    public static byte[] getFilePartition(String filename) {

        return new byte[5];
    }

    // 3. talk with master node
    public void sendMessageToMaster() {

    }


    // v2: 4. maintain a in memory, cache
}
