package lease;

import com.google.protobuf.ByteString;
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

       static  List<String> oldIpList = new ArrayList<>();
        public static void main(String args[]) {
            // monitor a single file
            System.out.println("Monitoring Lease file");
        }

        public  void copyList(List<String> newIpList) {
            oldIpList.clear();
            oldIpList = new ArrayList<>(newIpList);
        }

        public  void compareAndUpdate(List<String> newIpList) {
            System.out.println("comparing and updating");
            Set<String> set = new HashSet<>();
            for(String s: oldIpList) {
                set.add(s);
            }

            String server_id = null;
            String  server_port = null;

            StringBuffer sb = new StringBuffer();
            for(String s2 : newIpList) {
             sb.append(s2+",");
            }
            System.out.println("All new ips"+sb.toString());

            for(String s1: newIpList) {
                String ip = s1;
                System.out.println("Server ip now"+s1);
                try {
                    Properties prop = FetchConfig.getConfiguration(new File("../../conf/server.conf"));
                    server_id = prop.getProperty("server.id");
                    server_port = prop.getProperty("server.port");
                } catch (IOException ie) {
                    System.out.println("Unable to retrieve server config properties");
                    System.out.println("Exception: "+ie);
                }

                System.out.println("Server port: "+server_port);

                System.out.println("Server id: "+server_id);
                System.out.println("Ip is"+ip);

                ManagedChannel ch = ManagedChannelBuilder.forAddress(ip,Integer.parseInt(server_port.trim()) ).usePlaintext(true).build();
                RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
                if(!set.contains(s1)) {
                    // send hello to new node , if new node is added
                    System.out.println("Sending hello to new node: "+s1);
                    Route.Builder bld = Route.newBuilder();
                    bld.setId(1);
                    bld.setOrigin(Integer.parseInt(server_id));
                    bld.setPath("/update/from/dhcp/lease/new/node");

                    byte[] hello = ("HELLO new node!:"+ s1).getBytes();
                    bld.setPayload(ByteString.copyFrom(hello));

                    // blocking!
                    Route r = stub.request(bld.build());
                            // TODO response handling
                           String payload = new String(r.getPayload().toByteArray());
                           System.out.println("reply: " + r.getId() + ", from: " + r.getOrigin() + ", payload: " + payload);
             }
                // update all the nodes with current ips in the network ( if new node | one node is removed)
                System.out.println("Sending current ip updates in the network to all nodes");
                Route.Builder bld1 = Route.newBuilder();
                bld1.setId(1);
                bld1.setOrigin(Integer.parseInt(server_id));
                bld1.setPath("/update/from/dhcp/lease");
                byte[] hello = ("These are the current nodes in the network: "+ sb.toString()).getBytes();
                bld1.setPayload(ByteString.copyFrom(hello));
                // blocking!
                Route r = stub.request(bld1.build());
                // TODO response handling
                String payload = new String(r.getPayload().toByteArray());
                System.out.println("reply: " + r.getId() + ", from: " + r.getOrigin() + ", payload: " + payload);
            }
        }

        public  void monitorLease() {
            //Check for changes in dhcpd lease file
            TimerTask task = new Dhcp_Lease_Changes_Monitor(new File("/var/lib/dhcpd/dhcpd.leases")) {

                protected void onChange(File file) {
                    // here we code the action on a change
                    try {
                        //TODO: replace the command with relative path or use root dir
                        Process p = new ProcessBuilder("/home/vinod/cmpe275/demo1/275-project1-demo1/fetch_ip.sh").start();
                        BufferedReader reader1 = new BufferedReader(new InputStreamReader(p.getInputStream()));

                        List<String> newIpList = new ArrayList<>();
                        String output = null;
                        while((output = reader1.readLine())!= null) {
                            System.out.println("ip"+output);
                            newIpList.add(output);
                        }

                        compareAndUpdate(newIpList);
                        copyList(newIpList);

                    } catch (IOException io) {
                        System.out.println(io.getMessage());
                        System.out.println("io exception");
                    }

                }
            };
            Timer timer = new Timer();
            // repeat the check every second
            timer.schedule(task, new Date(), 1000);
        }
}
