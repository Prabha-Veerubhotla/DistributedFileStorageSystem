package gash.grpc.route;

import java.util.*;
import java.io.*;
public class Lease_Test {


        public static void main(String args[]) {
            // monitor a single file
            System.out.println("Monitoring Lease file");
        }

        public static void monitorLease() {
            TimerTask task = new Lease_Changes_Monitor(new File("/var/lib/dhcpd/dhcpd.lease")) {
                protected void onChange(File file) {
                    // here we code the action on a change
                    System.out.println("File " + file.getName() + " have change !");
                    try {
                        Process p = new ProcessBuilder("home/vinod/cmpe275/labs/untitled/src-dhcp-ip-list/fetch_ip.sh\"").start();
                        BufferedReader reader1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String line1 = null;
                        System.out.println("Printing all the ips1: ");
                        while ((line1 = reader1.readLine()) != null) {
                            System.out.println(line1 + "\n");
                        }
                    } catch (IOException io) {
                        System.out.println("io exception");
                    }
                }
            };
            Timer timer = new Timer();
            // repeat the check every second
            timer.schedule(task, new Date(), 1000);
        }
}
