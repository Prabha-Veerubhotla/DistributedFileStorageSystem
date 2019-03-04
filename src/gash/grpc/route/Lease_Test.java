package gash.grpc.route;

import java.util.*;
import java.io.*;
public class Lease_Test {


        public static void main(String args[]) {
            // monitor a single file
            TimerTask task = new Lease_Changes_Monitor( new File("/Users/prabha/Desktop/SEM_2019/275/Labs/275-project1-demo1/lease.txt") ) {
                protected void onChange( File file ) {
                    // here we code the action on a change
                    System.out.println( "File "+ file.getName() +" have change !" );
                    try {
                    new ProcessBuilder("grep -o '[0-9]\\{1,3\\}\\.[0-9]\\{1,3\\}\\.[0-9]\\{1,3\\}\\.[0-9]\\{1,3\\}' /var/lib/dhcpd/dhcpd.leases").start();
                    } catch (IOException io) {
                        System.out.println("io exception");
                    }

                }
            };

            Timer timer = new Timer();
            // repeat the check every second
            timer.schedule( task , new Date(), 1000 );
        }

}
