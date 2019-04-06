package lease;


import grpc.route.server.MasterNode;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.FileServiceGrpc;
import route.NodeInfo;
import route.NodeName;
import route.UpdateMessage;
import utility.FetchConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class Dhcp_Lease_Test {
    protected static Logger logger = LoggerFactory.getLogger("server-master");
    static List<String> oldIpList = new ArrayList<>();
    Map<String, List<String>> nodesInNetwork = new HashMap<>();
    List<String> newIpList = new ArrayList<>();


    public static void main(String args[]) {
        // monitor a single file
        logger.info("Monitoring DHCP Lease file");
    }

    public void copyList() {
        oldIpList.clear();
        oldIpList = new ArrayList<>(newIpList);
    }

    public void compareAndUpdate() throws StatusRuntimeException {
        logger.info("Comparing and Updating list of all the current nodes in the network");
        logger.info("new ip list: " + newIpList.toString());
        Set<String> set = new HashSet<>();
        for (String old : oldIpList) {
            set.add(old);
        }
        String server_port = "2345";

        StringBuffer sb = new StringBuffer();
        for (String ip : newIpList) {
            sb.append(ip + ",");
        }

        for (String newip : newIpList) {
            String ip = newip;
            try {
                //TODO: use relative path here
                Properties prop = FetchConfig.getConfiguration(new File("/home/vinod/cmpe275/WednesdayTest/275-project1/conf/server.conf"));
                server_port = prop.getProperty("server.port");
            } catch (IOException ie) {
                logger.info("Unable to retrieve server config properties, exception: " + ie);
            }

            ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, Integer.parseInt(server_port.trim())).usePlaintext(true).build();
            FileServiceGrpc.FileServiceBlockingStub blockingStub = FileServiceGrpc.newBlockingStub(ch);

            if (!set.contains(newip)) {
                logger.info("Sending hello to new node!");
                logger.info("New node ip is: " + newip);
                NodeInfo.Builder nodeInfo = NodeInfo.newBuilder();
                nodeInfo.setIp(newip);
                nodeInfo.setPort(server_port);

                String nodeName = blockingStub.assignNodeIp(nodeInfo.build()).getName();

                if (nodesInNetwork.containsKey(nodeName)) {
                    List<String> slaves = nodesInNetwork.get(nodeName);
                    slaves.add(newip);
                    nodesInNetwork.put(nodeName, slaves);
                } else {
                    List<String> nodes = new ArrayList<>();
                    nodes.add(newip);
                    nodesInNetwork.put(nodeName, nodes);
                }
                logger.info("reply: " + nodeName + ", from: " + newip);
            }
            logger.info("Sending current ip updates in the network to all nodes");
            UpdateMessage.Builder updateMessage = UpdateMessage.newBuilder();
            updateMessage.setMessage(sb.toString());

            UpdateMessage reply = blockingStub.nodeUpdate(updateMessage.build());

            logger.info("reply: " + reply.getMessage() + ", from: " + newip);
        }
    }

    public void monitorLease() {
        //Check for changes in dhcpd lease file
        TimerTask task = new Dhcp_Lease_Changes_Monitor(new File("/var/lib/dhcpd/dhcpd.leases")) {

            protected void onChange(File file) {
                // here we code the action on a change
                try {
                    //TODO: replace the command with relative path or use root dir
                    logger.info("Calling onchange dhcpd.lease file");
                    Process p = new ProcessBuilder("/home/vinod/cmpe275/demo1/275-project1-demo1/fetch_ip.sh").start();
                    BufferedReader reader1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    newIpList.clear();
                    logger.info("old ip list: " + newIpList.toString());
                    String output;
                    while ((output = reader1.readLine()) != null) {
                        newIpList.add(output);
                    }

                    compareAndUpdate();
                    copyList();

                } catch (IOException | StatusRuntimeException io) {
                    logger.info("Exception while handling changes of lease file: " + io);
                }

            }
        };
        Timer timer = new Timer();
        // repeat the check every second
        timer.schedule(task, new Date(), 1000);
    }

    public Map<String, List<String>> getCurrentNodeMapping() {
        return nodesInNetwork;
    }

    public void removeDeadnodes(List<String> deadNodes){
        logger.info("Removing dead nodes");
        newIpList.removeAll(deadNodes);
        copyList();
        MasterNode.removeDeadNodeStats(deadNodes);

    }

    public List<String> getCurrentIpList() {
        logger.info("old IP list size: "+ oldIpList);
        return oldIpList;
    }

    public boolean updateCurrentNodeMapping(NodeInfo nodeInfo, NodeName nodeName) {
        String nodename = nodeName.getName();
        logger.info("Adding: " + nodeInfo.getIp() + " to the current node list as " + nodename);
        if (nodesInNetwork.containsKey(nodename)) {
            List<String> clientlist = nodesInNetwork.get(nodename);
            clientlist.add(nodeInfo.getIp());
            nodesInNetwork.put(nodename, clientlist);

        } else {
            List<String> clientList = new ArrayList<>();
            clientList.add(nodeInfo.getIp());
            nodesInNetwork.put(nodename, clientList);
        }
        return true;
    }
}
