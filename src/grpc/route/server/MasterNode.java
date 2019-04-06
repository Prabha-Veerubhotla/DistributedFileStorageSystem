package grpc.route.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lease.Dhcp_Lease_Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MasterNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-master");
    static List<String> slaveip = new ArrayList<>();
    static Map<String,ManagedChannel> nodeIpChannelMap=new HashMap<>();
    static Map<String,Stats> nodeStatsMap=new HashMap<>();
    static String slave1port = "2345";
    static String slave1 = "localhost";
    private static ManagedChannel ch;
    private static FileServiceGrpc.FileServiceStub ayncStub;
    private static FileServiceGrpc.FileServiceBlockingStub blockingStub;
    private static String currentIP;
    private static int currentIPIxd = 0;
    private static int NOOFSHARDS = 1;
    private static boolean ackStatus;
    private static boolean done = false;
    static boolean isRoundRobinCalled = false;


    public static void setIsRoundRobinCalled(boolean isRoundRobinCalled) {
        MasterNode.isRoundRobinCalled = isRoundRobinCalled;
    }

    public static String assignSlaveIp(List<String> slaveiplist) {
        slaveip = slaveiplist;
        if(slaveiplist.size() != 0) {
            slave1 = slaveip.get(0);
        } else {
            slave1 = "localhost";
        }
        return slave1;
        //slave1 = "localhost"; // local testing
    }


    //Method for round robin IP - Sharding data among 3 Slaves
    public synchronized static String roundRobinIP() {
       logger.info("current ip list: "+slaveip);
        NOOFSHARDS = slaveip.size();
        logger.info("number of shards: "+NOOFSHARDS);
        currentIP = slaveip.get(currentIPIxd);
        currentIPIxd = (currentIPIxd + 1) % NOOFSHARDS;
        logger.info("returning ip: "+currentIP);
        return currentIP;
    }

    public static ManagedChannel createChannel(String slave1) {
        logger.info("creating channel for slave");
        logger.info("slave 1 ip is: "+slave1);
        ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        ayncStub = FileServiceGrpc.newStub(ch);
        logger.info("creating async stub ");
        blockingStub = FileServiceGrpc.newBlockingStub(ch);
        Node_ip_channel node_ip_channel=new Node_ip_channel();
        node_ip_channel.setIpAddress(slave1);
        node_ip_channel.setChannel(ch);
        nodeIpChannelMap.put(slave1,ch);
        return ch;
    }


    public static boolean streamFileToServer(FileData fileData, boolean complete, ManagedChannel channel) {
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
        ayncStub = FileServiceGrpc.newStub(channel);
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

    public static boolean updateFileToServer(FileData fileData, boolean complete) {
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

        StreamObserver<FileData> fileDataStreamObserver = ayncStub.updateFile(ackStreamObserver);

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

    public static boolean deleteFileFromServer(FileInfo fileInfo) {
        logger.info("deleting file: " + fileInfo.getFilename().getFilename());
        Ack ack = blockingStub.deleteFile(fileInfo);
        return ack.getSuccess();
    }


    // gets the hearbeat of all slaves and updates the nodeStatsMap.
    public static void getHeartBeatofAllSlaves(){
        logger.info("Fetching cpu and mem stats of slaves");
        Map<String,Stats> tempStats=new HashMap<>();
        //local testing.
        if(nodeIpChannelMap.isEmpty()){
            ManagedChannel channel=nodeIpChannelMap.get("localhost");
            blockingStub=FileServiceGrpc.newBlockingStub(channel);
            NodeInfo.Builder nodeInfo=NodeInfo.newBuilder();
            nodeInfo.setIp("localhost");
            nodeInfo.setPort("2345");
            Stats stats=blockingStub.isAlive(nodeInfo.build());
            logger.info("Got CPU stats from \"local-slave\" \n\tcpuUsage: "+stats.getCpuUsage()+"\n\tmemoryUsed: "+stats.getUsedMem()+"\n\tFreeSpace: "+stats.getDiskSpace());
        }

        nodeIpChannelMap.forEach((ip,channel1)->{
            blockingStub=FileServiceGrpc.newBlockingStub(channel1);

            NodeInfo.Builder nodeInfo=NodeInfo.newBuilder();
            nodeInfo.setIp(ip);
            nodeInfo.setPort("2345");
            Stats stats=blockingStub.isAlive(nodeInfo.build());
            tempStats.put(ip,stats);
            logger.info("Got CPU stats from slave:"+ip+" \n\tcpuUsage: "+stats.getCpuUsage()+"\n\tmemoryUsed: "+stats.getUsedMem()+"\n\tFreeSpace: "+stats.getDiskSpace());
        });
        updateNodeStats(tempStats);
    }

    public synchronized static void updateNodeStats(Map<String,Stats> newStats){
        Set<String> nodeSet=new HashSet<>();
        List<String> deadNodes=new ArrayList<>();

        newStats.forEach((ip,Stats)->{
            nodeSet.add(ip);
        });
        int numNewNodes=nodeSet.size();
        nodeStatsMap.forEach((ip,Stats)->{
            nodeSet.add(ip);
        });
        int numofNodeWentOff=nodeSet.size()-numNewNodes;
        String[] nodeArray= (String[]) nodeSet.toArray();

        if(numofNodeWentOff>0){
            for(int i=1;i<=numofNodeWentOff;i++) {
                deadNodes.add(nodeArray[nodeArray.length-i]);
            }
        }
        removeDeadSlavesFromDHCPList(deadNodes);
    }

    public synchronized static Map<String,Stats> getNodeStats() {
        return nodeStatsMap;
    }

    public synchronized static void removeDeadNodeStats(List<String> deadNodeIp) {
        for(String ip: deadNodeIp) {
            if (nodeStatsMap.containsKey(ip)) {
                nodeStatsMap.remove(ip);
            }
        }
    }


    public static void removeDeadSlavesFromDHCPList(List<String> deadNodes){
        new Dhcp_Lease_Test().removeDeadnodes(deadNodes);
    }
    // get the heartbeat and stats of individual node.
    public static Stats getHeartBeatofASlave(Node_ip_channel node_ip_channel){
            blockingStub=FileServiceGrpc.newBlockingStub(node_ip_channel.getChannel());
            NodeInfo.Builder nodeInfo=NodeInfo.newBuilder();
            nodeInfo.setIp(node_ip_channel.getIpAddress());
            nodeInfo.setPort("2345");
            Stats stats=blockingStub.isAlive(nodeInfo.build());
            logger.info("Got CPU stats from \"local-slave\" \n\tcpuUsage: "+stats.getCpuUsage()+"\n\tmemoryUsed: "+stats.getUsedMem()+"\n\tFreeSpace: "+stats.getDiskSpace());
        return stats;
    }


}

// 1. save meta data of files (which partition on which slave)

// 2. send heartbeat to slaves

// 3. hashing the data ( given file (parts) onto the 3 nodes)

// 4. replication of each part twice

// 5. update meta-data when a slave goes down or come up

// 6. take care of load balancing(data replication) when node goes up or down

// 7. talk with both client and other slaves -- done

