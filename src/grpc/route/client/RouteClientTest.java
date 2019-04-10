package grpc.route.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class RouteClientTest {
    String clientname;
    RouteClient rc;


    public RouteClientTest(String clientname, Properties p) {
        this.clientname = clientname;
        // perform all the operations
        this.rc = new RouteClient(p);
        rc.setName(clientname);
        rc.startClientSession();
    }

    public static void main(String[] args) {
        String clientname = "testUser1";
        String serverIp = "192.168.0.33";
        String serverPort = "9000";
        String filename = "files_for_testing/sample.txt";

        Properties p = new Properties();
        p.setProperty("host", serverIp);
        p.setProperty("port", serverPort);


        RouteClientTest routeClientTest = new RouteClientTest( clientname, p);

        routeClientTest.put(filename);







    }

    public boolean put(String filename) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Enter file path: ");
        try {
            String msg = br.readLine();
            System.out.println(rc.streamFileToServer(msg));
        } catch (IOException io) {

        }
        return true;
    }


}
