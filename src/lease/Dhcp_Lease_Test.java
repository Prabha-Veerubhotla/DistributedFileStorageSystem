package lease;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.FetchConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

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
    private List<String> msgTypes = FetchConfig.getMsgTypes();


    public static void main(String args[]) {
        // monitor a single file
        logger.info("Monitoring DHCP Lease file");
    }

    public void copyList() {
        oldIpList.clear();
        oldIpList = new ArrayList<>(newIpList);
    }

    public void compareAndUpdate() {
        logger.info("Comparing and Updating list of current nodes in the network");
        Set<String> set = new HashSet<>();
        for (String s : oldIpList) {
            set.add(s);
        }
        String server_port = null;

        StringBuffer sb = new StringBuffer();
        for (String s2 : newIpList) {
            sb.append(s2 + ",");
        }
        logger.info("All new ips: " + sb.toString());

        for (String s1 : newIpList) {
            String ip = s1;
            try {
                Properties prop = FetchConfig.getConfiguration(new File("/home/vinod/cmpe275/WednesdayTest/275-project1/conf/server.conf"));
                server_port = prop.getProperty("server.port");
            } catch (IOException ie) {
                logger.info("Unable to retrieve server config properties, exception: "+ie);
            }
            logger.info("Node Ip is: " + ip);
            logger.info("Node port: " + server_port);
            ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, Integer.parseInt(server_port.trim())).usePlaintext(true).build();
            RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

            if (!set.contains(s1)) {
                logger.info("Sending hello to new node!");
                logger.info("New node ip is: " + s1);

                Route.Builder bld = Route.newBuilder();
                bld.setOrigin("master");
                bld.setDestination(s1);
                bld.setPath("/update/from/dhcp/lease/new/node");
                byte[] ipmessage = s1.getBytes();
                bld.setType(msgTypes.get(7));
                bld.setPayload(ByteString.copyFrom(ipmessage));

                // blocking!
                Route r = stub.request(bld.build());
                // TODO response handling

                String payload = new String(r.getPayload().toByteArray());
                String nodereply = payload; // indicating whether node is a slave or client
                if (nodesInNetwork.containsKey(nodereply)) {
                    List<String> slaves = nodesInNetwork.get(nodereply);
                    slaves.add(s1);
                    nodesInNetwork.put(nodereply, slaves);
                } else {
                    List<String> nodes = new ArrayList<>();
                    nodes.add(s1);
                    nodesInNetwork.put(nodereply, nodes);
                }
                logger.info("reply: " + payload + ", from: " + r.getOrigin());
            }

            // update all the nodes with current ips in the network ( if new node | one node is removed)
            logger.info("Sending current ip updates in the network to all nodes");
            Route.Builder bld1 = Route.newBuilder();
            bld1.setOrigin("master");
            bld1.setType("update-nodes");
            bld1.setDestination(s1);
            bld1.setPath("/update/from/dhcp/lease");
            byte[] hello = ("These are the current nodes in the network: " + sb.toString()).getBytes();
            bld1.setPayload(ByteString.copyFrom(hello));
            // blocking!
            Route r = stub.request(bld1.build());
            // TODO response handling
            String payload = new String(r.getPayload().toByteArray());
            logger.info("reply: " + payload + ", from: " + r.getOrigin());
        }
    }

    public void monitorLease() {
        //Check for changes in dhcpd lease file
        TimerTask task = new Dhcp_Lease_Changes_Monitor(new File("/var/lib/dhcpd/dhcpd.leases")) {

            protected void onChange(File file) {
                // here we code the action on a change
                try {
                    //TODO: replace the command with relative path or use root dir
                    Process p = new ProcessBuilder("/home/vinod/cmpe275/demo1/275-project1-demo1/fetch_ip.sh").start();
                    BufferedReader reader1 = new BufferedReader(new InputStreamReader(p.getInputStream()));


                    String output = null;
                    while ((output = reader1.readLine()) != null) {
                        newIpList.add(output);
                    }

                    compareAndUpdate();
                    copyList();

                } catch (IOException io) {
                    logger.info("Exception while handling changes of lease file: "+io);
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

    public List<String> getCurrentIpList() {
        return newIpList;
    }

    public boolean updateCurrentNodeMapping(Route r, String ip) {
        String nodename = r.getPayload().toString();
        logger.info("adding: " + ip + " to the current node list as " + nodename);
        if (nodesInNetwork.containsKey(nodename)) {
            List<String> clientlist = nodesInNetwork.get(nodename);
            clientlist.add(ip);
            nodesInNetwork.put(nodename, clientlist);

        } else {
            List<String> clientList = new ArrayList<>();
            clientList.add(ip);
            nodesInNetwork.put(nodename, clientList);
        }
        return true;
    }
}
