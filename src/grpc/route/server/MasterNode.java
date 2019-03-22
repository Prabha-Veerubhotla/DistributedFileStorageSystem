package grpc.route.server;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import route.RouteServiceGrpc;

import java.util.*;

public class MasterNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-master");

    static List<String> slaveip = new ArrayList<>();
    static String slave1port = "2346";
    static String slave1 = null;
    private static ManagedChannel ch;
    private static RouteServiceGrpc.RouteServiceBlockingStub stub;
    private static String myIp;
    private static String username;

    public static void setMasterIp(String ip) {
        myIp = ip;
    }

    public static void setUsername(String name) {
        username = name;
    }

    public static void assignSlaveIp(List<String> slaveiplist) {
        slaveip = slaveiplist;
        //slave1 = slaveip.get(0);
        slave1 = "127.0.0.1";
    }

    public static Route sendMessageToSlaves(String type, String path, String payload) {
        ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        stub = RouteServiceGrpc.newBlockingStub(ch);
        Route.Builder bld = Route.newBuilder();
        bld.setUsername(username);
        bld.setOrigin(myIp);
        bld.setDestination(slave1);
        bld.setPayload(ByteString.copyFrom(payload.getBytes()));
        bld.setType(type);
        bld.setPath(path);
        Route r = stub.request(bld.build());
        return r;
    }

    public static boolean put(Route r) {
        logger.info("Saving message in node: " + slave1);
        String path = r.getPath();
        String type = r.getType();
        String payload = r.getPayload().toString();
        r = sendMessageToSlaves(type, path, payload);
        payload = new String(r.getPayload().toByteArray());
        logger.info("Received response from slave node: " + payload);
        if (payload.equalsIgnoreCase("success")) {
            return true;
        }
        return false;
    }

    public static byte[] get(Route r) {
        logger.info("retrieving message from  node: " + slave1);
        String type = r.getType();
        String payload = r.getPayload().toString();
        String path = r.getPath();
        r = sendMessageToSlaves(type, path, payload);
        logger.info("Received response from slave node: " + payload);
        return r.getPayload().toByteArray();
    }

    public static String list(Route r) {
        logger.info("retrieving all message of user: " + r.getUsername() + " from  node: " + slave1);
        String type = r.getType();
        String payload = r.getPayload().toString();
        String path = r.getPath();
        r = sendMessageToSlaves(type, path, payload);
        logger.info("Received response from slave node: " + payload);
        logger.info("list of messages or files are: " + new ArrayList<>(Arrays.asList(payload.split(","))));
        return r.getPayload().toString();
    }

    public static boolean delete(Route r) {
        logger.info("deleting message or file: " + r.getPayload() + " from:" + slave1);
        String path = r.getPath();
        String type = r.getType();
        String payload = r.getPayload().toString();
        r = sendMessageToSlaves(type, path, payload);
        payload = new String(r.getPayload().toByteArray());
        logger.info("Received response from slave node: " + payload);
        if (payload.equalsIgnoreCase("success")) {
            return true;
        }
        return false;
    }

    public static String sendIpToNode(Map<String, List<String>> map, List<String> ipList) {
        //TODO: modify to accommodate client or slave ip
        String clientIp;
        List<String> slaveList = new ArrayList<>();
        if (map.containsKey("slave")) {
            slaveList = map.get("slave");
        }
        Set<String> myset = new HashSet<>();
        for (String s : slaveList) {
            myset.add(s);
        }
        for (String s : ipList) {
            myset.add(s);
        }
        Object[] array1 = myset.toArray();
        clientIp = (String) array1[array1.length - 1];
        return clientIp;
    }
}


   /* public static boolean saveFile(String filename, String name, String payload) {

        ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
        Route.Builder bld = Route.newBuilder();
        logger.info("received file in master...");
        bld.setOrigin("master");
        bld.setDestination("slave");
        // bld.setType("file-put");
        bld.setPath(filename);
        bld.setType("file-put");
        bld.setUsername(name);
        logger.info("sending file to slave...");
        bld.setPayload(ByteString.copyFrom(payload.getBytes()));

        Route r = stub.request(bld.build());
        if (new String(r.getPayload().toByteArray()).equalsIgnoreCase("success")) {
            return true;
        }
        logger.info("slave saved file successfully");
        return false;
    }

      public static boolean deleteMessage(String msg, String name) {
        boolean result = false;
        ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

        // send hello to new node , if new node is added
        logger.info("deleting message from  node: " + slave1);
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
        if (status.equalsIgnoreCase("success")) {
            result = true;
        }
        return result;
    }

    public static String listMessages(String name) {

        //List<String> stringList = new ArrayList<>();
        ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

        // send hello to new node , if new node is added
        logger.info("listing messages from  node: " + slave1);
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath("/list/message/from/node");
        bld.setType("message-list");
        bld.setUsername(name);

        // blocking!
        Route r = stub.request(bld.build());
        String status = new String(r.getPayload().toByteArray());
        return status;
    }*/


// 1. save meta data of files (which partition on which slave)

// 2. send heartbeat to slaves

// 3. hashing the data ( given file (parts) onto the 3 nodes)

// 4. replication of each part twice

// 5. update meta-data when a slave goes down or come up

// 6. take care of load balancing(data replication) when node goes up or down

// 7. talk with both client and other slaves

