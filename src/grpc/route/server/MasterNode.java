package grpc.route.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lease.Dhcp_Lease_Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    static MasterMetaData masterMetaData=new MasterMetaData();

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

    public static Map<String, Double> calculateSlaveStatsScore() {
        double cpuWeight = 0.4;
        double diskWeight = 0.5;
        double memWeight = 0.1;
        double mem = 0.0;
        Map<String, Double> scoreMap = new HashMap<>();
        logger.info("In claculateSlaveStatsScore: nodeStatsMapSize: "+nodeStatsMap.size());
        for(Map.Entry<String, Stats> m : nodeStatsMap.entrySet()) {

            double cpu = Double.parseDouble(m.getValue().getCpuUsage());
            if (m.getValue().getUsedMem() != "") {
                mem = Double.parseDouble(m.getValue().getUsedMem());
            }

            double disk = Double.parseDouble(m.getValue().getDiskSpace());

            double score = cpuWeight*cpu + diskWeight*disk + memWeight*mem;
            logger.info("stats score: "+score+" for ip: "+m.getKey());
            scoreMap.put(m.getKey(), score);
        }
        return scoreMap;
    }


    //Method for round robin IP - Sharding data among 3 Slaves
    public synchronized static String roundRobinIP() {
       logger.info("current ip list: "+new Dhcp_Lease_Test().getCurrentIpList());
        List<String> currentList = new Dhcp_Lease_Test().getCurrentIpList();
        NOOFSHARDS =currentList.size();
        logger.info("number of shards: "+NOOFSHARDS);
        currentIP = currentList.get(currentIPIxd);
        currentIPIxd = (currentIPIxd + 1) % NOOFSHARDS;

        /* using heartbeat stats also
        Map<String, Double> map = calculateSlaveStatsScore();
        logger.info("Calculating slave stats list size: "+map.size());
        if(map.containsKey(currentIP)) {
            logger.info("stats score: "+ map.get(currentIP)+" for current ip: "+currentIP);
            if(map.get(currentIP) > 0.8) {
                return roundRobinIP();
            }
        }*/
        logger.info("Returning ip: "+currentIP);
        return currentIP;
    }

    public static ManagedChannel createChannel(String slave1) {
        logger.info("creating channel for slave");
        logger.info("slave 1 ip is: "+slave1);
        ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        ayncStub = FileServiceGrpc.newStub(ch);
        logger.info("creating async stub ");
        blockingStub = FileServiceGrpc.newBlockingStub(ch);
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
        logger.info("getting current ip list from dhcp lease file");
        List<String> currentIpList = new Dhcp_Lease_Test().getCurrentIpList();
        for(String ip: currentIpList) {
            Node_ip_channel node_ip_channel = new Node_ip_channel();
            node_ip_channel.setIpAddress(ip);
            ManagedChannel ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
            node_ip_channel.setChannel(ch);
            nodeIpChannelMap.put(ip, ch);
        }
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
        logger.info("In node stats");
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
        String[] nodeArray=  nodeSet.toArray(new String[nodeSet.size()]);
        if(numofNodeWentOff>0){
            for(int i=1;i<=numofNodeWentOff;i++) {
                deadNodes.add(nodeArray[nodeArray.length-i]);
            }
        }
        if(deadNodes.size()!=0) {
            removeDeadSlavesFromDHCPList(deadNodes);

        }
        nodeStatsMap.putAll(newStats);

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

    public static void migrateDataFromANode(String nodeIP){
        Map<String,List<String>> userFile=masterMetaData.getMetaDataForIP(nodeIP);

        AtomicReference<List<String>> nodesNottobeReplicated=null;

        userFile.forEach((username,fileList)->{
            for(int i=0;i<fileList.size();i++) {
                nodesNottobeReplicated.set(masterMetaData.getMetaData(username, fileList.get(i)));
                nodesNottobeReplicated.get().remove(nodeIP);
                //getting file data from the node that already has the file.
                FileInfo.Builder fileInfo=FileInfo.newBuilder();
                fileInfo.setFilename(fileList.get(i));

//                SlaveNode.get(fileInfo);
//                String IPtoReplicateTo=null;
//                while(true){
//                    IPtoReplicateTo=roundRobinIP();
//                    if(!nodesNottobeReplicated.get().contains(IPtoReplicateTo))
//                        break;
//                }
//                ManagedChannel ch=ManagedChannelBuilder.forAddress(IPtoReplicateTo,Integer.parseInt(slave1port)).usePlaintext().build();
//                ayncStub = FileServiceGrpc.newStub(ch);





                /*CountDownLatch cdl = new CountDownLatch(1);
                StreamObserver<Ack> ackStreamObserver = new StreamObserver<Ack>() {

                    @Override
                    public void onNext(Ack ack) {
                        ackStatus = ack.getSuccess();
                        logger.info("Received ack status from the replicated server when a node is dead: " + ack.getSuccess());
                        logger.info("Received ack  message from the replicated server when a node is dead: " + ack.getMessage());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.info("Exception in the response from server that replicating data when a node is dead: " + throwable);
                        cdl.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("Server is done sending data");
//                        putCompleted = true;
                        cdl.countDown();
                    }
                };

                route.FileData.Builder fileData = FileData.newBuilder();

                route.FileResponse.Builder fileResponse = FileResponse.newBuilder().setFilename(fileList.get(i));
                fileData.setFilename(fileResponse.build());

                route.UserInfo.Builder userInfo = UserInfo.newBuilder().setUsername(username);
                fileData.setUsername(userInfo.build());

                StreamObserver<FileData> fileDataStreamObserver = ayncStub.uploadFile(ackStreamObserver);

                if (checkIfFile(filename)) {
                    logger.info(filename + " is a file");
                    File fn = new File(filename);
                    logger.info("file length is: "+fn.length());
                    FileInputStream fis = null;
                    try {
                        fis = new FileInputStream(fn);
                        long seq = 0l;
                        final int blen = 4194000;
                        byte[] raw = new byte[blen];
                        boolean done = false;
                        while (!done) {
                            int n = fis.read(raw, 0, blen);
                            logger.info("n: "+n);
                            if (n <= 0)
                                break;
                            // identifying sequence number
                            seq++;
                            logger.info("Streaming seq num: " + seq);
                            fileData.setContent(ByteString.copyFrom(raw, 0, n));
                            logger.info("seq num is: " + seq);
                            fileData.setSeqnum(seq);
                            logger.info("Sending file data to server with seq num: " + seq);
                            fileDataStreamObserver.onNext(fileData.build());
                        }
                    } catch (IOException e) {
                        ; // ignore? really?
                        fileDataStreamObserver.onError(e);
                    } finally {
                        try {
                            fis.close();
                        } catch (IOException e) {
                            ; // ignore
                        }
                    }
                }
                fileDataStreamObserver.onCompleted();

                try {
                    cdl.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    logger.info("Exception while waiting for count down latch: " + ie);
                }
            }

            while(!putCompleted) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }

            if (ackStatus) {
                return "success";
            }
            return "Unable to save file";




            }

        });
*/

    }



    /* get the heartbeat and stats of individual node.
    public static Stats getHeartBeatofSelectedSlaves(List<String> nodes){
            blockingStub=FileServiceGrpc.newBlockingStub(node_ip_channel.getChannel());
            NodeInfo.Builder nodeInfo=NodeInfo.newBuilder();
            nodeInfo.setIp(node_ip_channel.getIpAddress());
            nodeInfo.setPort("2345");
            Stats stats=blockingStub.isAlive(nodeInfo.build());
            logger.info("Got CPU stats from \"local-slave\" \n\tcpuUsage: "+stats.getCpuUsage()+"\n\tmemoryUsed: "+stats.getUsedMem()+"\n\tFreeSpace: "+stats.getDiskSpace());
        return stats;
    }*/


}

// 1. save meta data of files (which partition on which slave)-- done

// 2. send heartbeat to slaves -TODO

// 3. hashing the data --done

// 4. replication of each part twice -- done

// 5. update meta-data when a slave goes down or come up  - TODO

// 6. take care of load balancing(data replication) when node goes up or down - TODO

// 7. talk with both client and other slaves -- done

