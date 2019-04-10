package grpc.route.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class RouteClientTest {
    protected static Logger logger = LoggerFactory.getLogger("RouteClientTest");
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

        // put test
        long startTime = System.currentTimeMillis();
        routeClientTest.put(filename, 0);
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        timeTaken = timeTaken/1000;
        logger.info("Time taken to save the file  "+filename +" in server in seconds: "+ timeTaken);


        //get test
        startTime = System.currentTimeMillis();
        routeClientTest.get(filename);
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        timeTaken = timeTaken/1000;
        logger.info("Time taken to get the file  "+filename +"  in seconds:" + timeTaken);


        //list test
        startTime = System.currentTimeMillis();
        routeClientTest.list();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        timeTaken = timeTaken/1000;
        logger.info("Time taken to list the files of user: "+clientname+" in seconds: " + timeTaken);


        //search test
        startTime = System.currentTimeMillis();
        routeClientTest.search(filename);
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        timeTaken = timeTaken/1000;
        logger.info("Time taken to search if the file  "+filename +"  is present in seconds:: " + timeTaken);


        //update test
        startTime = System.currentTimeMillis();
        routeClientTest.update(filename);
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        timeTaken = timeTaken/1000;
        logger.info("Time taken to update the file  "+filename +" already saved in seconds: " + timeTaken);


        //delete test
        startTime = System.currentTimeMillis();
        routeClientTest.delete(filename);
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        timeTaken = timeTaken/1000;
        logger.info("Time taken to delete the file  "+filename +"  in seconds: " + timeTaken);


        //put test 100 times
        startTime = System.currentTimeMillis();
        int n = 100;
        routeClientTest.putMultipleTimes(filename, n);
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        timeTaken = timeTaken/1000;
        logger.info("Time taken to save the file  "+filename +" " + n +" times in seconds:"+ timeTaken);

    }

    public boolean put(String filename, int n) {
        String putStatus = "";

            putStatus = rc.streamFileToServer(filename,  n);
            logger.info(putStatus);

        if(putStatus.equalsIgnoreCase("success")){
            return true;
        }
        return false;
    }

    public boolean putMultipleTimes(String filename, int n) {
        for(int i = 1 ; i <=n;i++) {
            if(!put(filename, i)) {
                return false;
            }
        }
        return true;
    }

    public File get(String filename) {
        return  rc.getFileFromServer(filename);
    }

    public boolean delete(String filename) {
        if(rc.deleteFileFromServer(filename).equalsIgnoreCase("success")) {
            return true;
        }
        return false;
    }

    public boolean search(String filename) {
        return rc.searchFileInServer(filename);
    }

    public String list() {
        return rc.listFilesInServer(clientname);
    }

    public boolean update(String filename) {
        String updateStatus = "";

        updateStatus = rc.updateFileInServer(filename);
        logger.info(updateStatus);

        if(updateStatus.equalsIgnoreCase("success")){
            return true;
        }
        return false;
    }



}
