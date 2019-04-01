package grpc.route.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MasterNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-master");
    static List<String> slaveip = new ArrayList<>();
    static String slave1port = "2345";
    static String slave1 = null;
    private static ManagedChannel ch;
    private static FileServiceGrpc.FileServiceStub ayncStub;
    private static FileServiceGrpc.FileServiceBlockingStub blockingStub;
    private static String currentIP;
    private static int currentIPIxd = 0;
    private static int NOOFSHARDS = 3;
    private static boolean ackStatus;
    private static FileData result;
    private static boolean next = false;
    private static boolean done = false;


    public static void assignSlaveIp(List<String> slaveiplist) {
        slaveip = slaveiplist;
        slave1 = slaveip.get(0);
        //slave1 = "localhost"; // local testing

        //TODO: create channels for all the slaves

    }

    //Method for round robin IP - Sharding data among 3 Slaves
    public synchronized static String roundRobinIP() {
        currentIP = slaveip.get(currentIPIxd);
        currentIPIxd = (currentIPIxd + 1) % NOOFSHARDS;
        return currentIP;
    }

    public static void createChannel() {
        logger.info("creating channel for slave");
        ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        ayncStub = FileServiceGrpc.newStub(ch);
        blockingStub = FileServiceGrpc.newBlockingStub(ch);
    }


    public static boolean streamFileToServer(FileData fileData, boolean complete) {
        CountDownLatch cdl = new CountDownLatch(1);
        StreamObserver<Ack> ackStreamObserver = new StreamObserver<Ack>() {

            @Override
            public void onNext(Ack ack) {
                ackStatus = ack.getSuccess();
                logger.info("Received ack status from the server: " + ack.getSuccess());
                logger.info("Received ack  message from the server: " + ack.getMessage());
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the response from server: " + throwable);
                cdl.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Server is done sending data");
                cdl.countDown();
            }
        };

        StreamObserver<FileData> fileDataStreamObserver = ayncStub.uploadFile(ackStreamObserver);

        if (complete) {
            fileDataStreamObserver.onNext(fileData);
            logger.info("sending completed to slave");
            fileDataStreamObserver.onCompleted();
        } else {
            fileDataStreamObserver.onNext(fileData);
            logger.info("sent data with seq num:  "+fileData.getSeqnum()+" to slave");
        }
        try {
            cdl.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Exception while waiting for count down latch: " + ie);
        }
        return ackStatus;
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

    public static boolean deleteFileFromServer(FileInfo fileInfo) {
        logger.info("deleting file: " + fileInfo.getFilename().getFilename());
        Ack ack = blockingStub.deleteFile(fileInfo);
        return ack.getSuccess();
    }

    public static boolean searchFileInServer(FileInfo fileInfo) {
        logger.info("searching file: " + fileInfo.getFilename().getFilename());
        Ack ack = blockingStub.searchFile(fileInfo);
        return ack.getSuccess();
    }

    public static String listFilesInServer(UserInfo userInfo) {
        logger.info("listing files of user : " + userInfo.getUsername());
        FileResponse fileResponse = blockingStub.listFile(userInfo);
        return fileResponse.getFilename();
    }


    public static boolean checkDoneStatus() {
        while(!done){
            return false;
        }
        return true;
    }


}

// 1. save meta data of files (which partition on which slave)

// 2. send heartbeat to slaves

// 3. hashing the data ( given file (parts) onto the 3 nodes)

// 4. replication of each part twice

// 5. update meta-data when a slave goes down or come up

// 6. take care of load balancing(data replication) when node goes up or down

// 7. talk with both client and other slaves -- done

