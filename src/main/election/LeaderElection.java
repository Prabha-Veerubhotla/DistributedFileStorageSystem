package main.election;

import grpc.route.server.MasterNode;
import grpc.route.server.Node_ip_channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lease.Dhcp_Lease_Test;
import main.db.RedisHandler;
import route.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.logging.Logger;

public class LeaderElection extends FileServiceGrpc.FileServiceImplBase {
    Logger logger = Logger.getLogger(RedisHandler.class.getName());
    Dhcp_Lease_Test dhcp_lease_test = new Dhcp_Lease_Test();
    String nodePort = "2345";
    Map<String,ManagedChannel> nodeIpChannelMap=new HashMap<>();
    FileServiceGrpc.FileServiceBlockingStub blockingStub;
    List<NodeInfo> ips = new ArrayList<>();
    private final static int NODES = 4;
    NodeInfo leaderNode = null;
    NodeInfo selfIP = null;

    LeaderElection(){
//        TODO: add self IP before running
        ips.add(selfIP);
    }

    public NodeInfo electLeader(NodeInfo node1, NodeInfo node2){
        if(node1 == null)
            return node2;
        String[] node1List = node1.getIp().split(".");
        String[] node2List = node2.getIp().split(".");
        if(Integer.parseInt(node1List[node1List.length - 1]) < Integer.parseInt(node2List[node2List.length - 1])){
            return node1;
        }
        return node2;
    }

    @Override
    public void vote(NodeInfo request, StreamObserver<Ack> responseObserver) {
        super.vote(request, responseObserver);
        ips.add(request);
        if (ips.size()  == NODES) {
            for(NodeInfo ip: ips){
                leaderNode = electLeader(leaderNode, ip);
            }
        }
    }

    public void sendVote() {
        logger.info("getting current ip list");
        List<String> currentIpList = new ArrayList<>();
        try {
            Scanner s = new Scanner(new File("../../../raftIPList.txt"));
            while (s.hasNextLine()){
                currentIpList.add(s.nextLine());
            }
            s.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for(String ip: currentIpList) {
            Node_ip_channel node_ip_channel = new Node_ip_channel();
            node_ip_channel.setIpAddress(ip);
            ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, Integer.parseInt(nodePort.trim())).usePlaintext(true).build();
            node_ip_channel.setChannel(ch);
            nodeIpChannelMap.put(ip, ch);
        }

        nodeIpChannelMap.forEach((ip,channel1)->{
            blockingStub=FileServiceGrpc.newBlockingStub(channel1);
            NodeInfo.Builder nodeInfo=NodeInfo.newBuilder();
            nodeInfo.setIp(ip);
            nodeInfo.setPort(nodePort);
            blockingStub.vote(nodeInfo.build());
        });
    }

}
