package grpc.route.server;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import route.RouteServiceGrpc;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MasterNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-master");
    static List<String> slaveip = new ArrayList<>();
    static String slave1port = "2345";
    static String slave1 = null;
    private static ManagedChannel ch;
    private static RouteServiceGrpc.RouteServiceStub stub;
    private static String myIp;
    private static String username;
    private static Route response;
    private static String currentIP;
    private static int currentIPIxd = 0;
    private static int NOOFSHARDS = 3;

    public static void setMasterIp(String ip) {
        myIp = ip;
    }

    public static void setUsername(String name) {
        username = name;
    }

    public static void assignSlaveIp(List<String> slaveiplist) {
        slaveip = slaveiplist;
        slave1 = slaveip.get(0);
        //slave1 = "localhost"; // local testing

    }

    //Method for round robin IP - Sharding data among 3 Slaves
    public synchronized static String roundRobinIP(){
        currentIP = slaveip.get(currentIPIxd);
        currentIPIxd = (currentIPIxd + 1) % NOOFSHARDS;
        return currentIP;
    }

    // send any message to slave
    public static void sendMessageToSlaves(Route r) {
        ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        stub = RouteServiceGrpc.newStub(ch);
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<Route> requestObserver = stub.request(new StreamObserver<Route>() {
            //handle response from server here
            @Override
            public void onNext(Route route) {
                logger.info("Received response from slave: " + route.getPayload());
                response = route;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the response from slaves: " + throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Slave is done sending data.!");
                latch.countDown();
            }
        });

        Route.Builder bld = Route.newBuilder();
        bld.setUsername(username);
        bld.setOrigin(myIp);
        //MasterMetaData Methods (userName, fileName, seqID, IP);
        bld.setDestination(slave1);
        bld.setPayload(ByteString.copyFrom(r.getPayload().toByteArray()));
        bld.setType(r.getType());
        bld.setPath(r.getPath());
        bld.setSeq(r.getSeq());

        requestObserver.onNext(bld.build());
        //requestObserver.onCompleted();
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Exception while waiting for count down latch: " + ie);
        }
    }

    // collecting data(chunks) from slaves -- for get call
    public static Route collectDataFromSlaves(Route r) {
        ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        stub = RouteServiceGrpc.newStub(ch);
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<Route> requestObserver = stub.request(new StreamObserver<Route>() {
            //handle response from server here
            @Override
            public void onNext(Route route) {
                logger.info("Received response from slave: " + route.getPayload());
                response = route.toBuilder().build();
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the response from slave: " + throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Slave is done sending data");
                latch.countDown();
            }
        });

        Route.Builder bld = Route.newBuilder();
        bld.setUsername(username);
        bld.setOrigin(myIp);
        bld.setDestination(slave1);
        bld.setPayload(ByteString.copyFrom(r.getPayload().toByteArray()));
        bld.setType(r.getType());
        logger.info("request type is: "+r.getType());
        bld.setPath(r.getPath());
        bld.setSeq(r.getSeq());
        logger.info("Sending request to slave to retrieve file: " + r.getPath());
        requestObserver.onNext(bld.build());
        //if(!bld.getType().equalsIgnoreCase("get")) {
        requestObserver.onCompleted();
        //}
        return response;
    }

    public static boolean put(Route r) {
        logger.info("Saving file in node: " + slave1);
        logger.info("sending file to slave with seq num: " + r.getSeq());
        sendMessageToSlaves(r);
        String payload = new String(response.getPayload().toByteArray());
        logger.info("Received response from slave node: " + payload);
        if (payload.equalsIgnoreCase("success")) {
            return true;
        }
        return false;
    }

    public static byte[] get(Route r) {
        logger.info("retrieving message from  node: " + slave1);
        sendMessageToSlaves(r);
        String payload = new String(response.getPayload().toByteArray());
        logger.info("Received response from slave node: " + payload);
        return response.getPayload().toByteArray();
    }

    public static String list(Route r) {
        logger.info("retrieving all message of user: " + r.getUsername() + " from  node: " + slave1);
        sendMessageToSlaves(r);
        String payload = new String(response.getPayload().toByteArray());
        logger.info("Received response from slave node: " + payload);
        logger.info("list of messages or files are: " + new ArrayList<>(Arrays.asList(payload.split(","))));
        return response.getPayload().toString();
    }

    public static boolean delete(Route r) {
        logger.info("deleting message or file: " + r.getPayload() + " from:" + slave1);
        sendMessageToSlaves(r);
        String payload = new String(response.getPayload().toByteArray());
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

// 1. save meta data of files (which partition on which slave)

// 2. send heartbeat to slaves

// 3. hashing the data ( given file (parts) onto the 3 nodes)

// 4. replication of each part twice

// 5. update meta-data when a slave goes down or come up

// 6. take care of load balancing(data replication) when node goes up or down

// 7. talk with both client and other slaves -- done

