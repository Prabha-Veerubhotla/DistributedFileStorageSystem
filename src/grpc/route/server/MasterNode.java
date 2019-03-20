package grpc.route.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

import java.util.ArrayList;
import java.util.List;

public class MasterNode extends RouteServerImpl {

    static String slave1ip = "127.0.0.1";
    static String slave1port = "2346";
    static String origin = "master";
    static String destination = "slave";


    public static boolean saveMessage(String msg, String name) {
        //save message in node-1

        ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1ip,Integer.parseInt(slave1port.trim()) ).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

            // send hello to new node , if new node is added
            System.out.println("Sending message to node: "+slave1ip);
            Route.Builder bld = Route.newBuilder();
            bld.setOrigin(origin);
            bld.setDestination(destination);
            bld.setPath("/save/message/in/node");
            bld.setType("message-save");
            bld.setUsername(name);
            byte[] hello = msg.getBytes();
            bld.setPayload(ByteString.copyFrom(hello));


            // blocking!
            Route r = stub.request(bld.build());
            String payload = new String(r.getPayload().toByteArray());
            logger.info("received response from slave node: "+ payload);
            if(payload.equalsIgnoreCase("success")) {
                return true;
            }
            return false;

    }

    public static String getMessage(String msg, String name) {
       String result = null;
       //read from metadata
        ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1ip,Integer.parseInt(slave1port.trim()) ).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

        // send hello to new node , if new node is added
        System.out.println("retrieving message from  node: "+slave1ip);
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath("/retrieve/message/from/node");
        bld.setType("message-get");
        bld.setUsername(name);
        byte[] hello = msg.getBytes();
        bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = stub.request(bld.build());
        result = new String(r.getPayload().toByteArray());

       return result;
    }

    public static boolean deleteMessage(String msg, String name) {
        boolean result = false;
        ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1ip,Integer.parseInt(slave1port.trim()) ).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

        // send hello to new node , if new node is added
        System.out.println("deleting message from  node: "+slave1ip);
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath("/delete/message/from/node");
        bld.setType("message-delete");
        bld.setUsername(name);
        byte[] hello = msg.getBytes();
        bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = stub.request(bld.build());
        String status = new String(r.getPayload().toByteArray());
        if(status.equalsIgnoreCase("success")) {
            result = true;
        }
        return result;
    }

    public static String listMessages(String name) {

        //List<String> stringList = new ArrayList<>();
        ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1ip,Integer.parseInt(slave1port.trim()) ).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

        // send hello to new node , if new node is added
        System.out.println("listing messages from  node: "+slave1ip);
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath("/list/message/from/node");
        bld.setType("message-list");
        bld.setUsername(name);
        //byte[] hello = msg.getBytes();
        //bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = stub.request(bld.build());
        String status = new String(r.getPayload().toByteArray());
        return status;
    }



    // 1. save meta data of files (which partition on which slave)

    // 2. send heartbeat to slaves

    // 3. hashing the data ( given file (parts) onto the 3 nodes)

    // 4. replication of each part twice

    // 5. update meta-data when a slave goes down or come up

    // 6. take care of load balancing(data replication) when node goes up or down

    // 7. talk with both client and other slaves
}
