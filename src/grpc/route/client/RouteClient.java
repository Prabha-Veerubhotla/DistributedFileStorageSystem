package grpc.route.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import route.RouteServiceGrpc;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import utility.FetchConfig;

/**
 * copyright 2018, gash
 * <p>
 * Gash licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

//TODO: make all calls aynschronous
//TODO: listen continuously for messages from server on a background thread

public class RouteClient {
    private static ManagedChannel ch;
    private static RouteServiceGrpc.RouteServiceBlockingStub stub;
    private Properties setup;
    private String name;
    private static String myIp = "client"; // intially , later master node will assign an ip
    protected static Logger logger = LoggerFactory.getLogger("client");
    private List<String> msgTypes = new ArrayList<>();

    public RouteClient(Properties setup) {
        this.setup = setup;
    }

    public void setName(String clientName) {
        name = clientName;
        logger.info("setting client name as: "+name);
    }

    public String getName() {
        return name;
    }

    public void startClientSession() {
        String host = setup.getProperty("host");
        String port = setup.getProperty("port");
        if (host == null || port == null) {
            throw new RuntimeException("Missing port and/or host");
        }
        ch = ManagedChannelBuilder.forAddress(host, Integer.parseInt(port)).usePlaintext(true).build();
        //TODO: make it async stub
        stub = RouteServiceGrpc.newBlockingStub(ch);
        System.out.println("Client running...");
        msgTypes = FetchConfig.getMsgTypes();
        //request ip from node running dhcp-server
        requestIp();
        //reply node info stating that you are client
        sendNodeInfo();
    }

    public Route sendMessageToServer(String type, String path, String payload) {
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(myIp);
        bld.setDestination(setup.getProperty("host")); // from the args , when we start client
        bld.setType(type);
        bld.setUsername(name);
        bld.setPath(path);
        bld.setPayload(ByteString.copyFrom(payload.getBytes()));
        // convert string to byte string,
        // to be compatible with protobuf format

        // blocking!
        Route r = RouteClient.stub.request(bld.build());
        return r;
    }

    public boolean join() {
        String type = msgTypes.get(0);
        String payload = "joining";
        String path = "/client/joining";
        Route r = sendMessageToServer(type, path, payload);
        logger.info("reply from master node: " + new String(r.getPayload().toByteArray()));
        if (new String(r.getPayload().toByteArray()).equalsIgnoreCase("welcome")) {
            return true;
        }
        return false;
    }

    public void stopClientSession() {
        ch.shutdown();
    }

    public void requestIp() {
        String type = msgTypes.get(5);
        String path = "requesting/client/ip";
        String payload = "/requesting";
        Route r = sendMessageToServer(type, path, payload);
        myIp = new String(r.getPayload().toByteArray());
    }

    public void sendNodeInfo() {
        String type = msgTypes.get(6);
        String path = "sending/node/info";
        String payload = "client";
        Route r = sendMessageToServer(type, path, payload);
        if (new String(r.getPayload().toByteArray()).equalsIgnoreCase("success")) {
            logger.info("Got node information from master node");
            logger.info("my ip is: " + myIp);
        }
    }

    public byte[] convertFileContent(String filename) {
        RandomAccessFile f;
        try {
            try {
                f = new RandomAccessFile(filename, "r");
            } catch (FileNotFoundException fe) {
                //if file is not found
                // if it is msg, return byte array
                logger.info("File: " + filename + " not found");
                logger.info("Saving it as message");
                return filename.getBytes();
            }
            logger.info("file length is: "+f.length());
            byte[] b = new byte[(int) f.length()];
            f.readFully(b);
            f.close();
            return b;
        } catch (IOException ie) {
            logger.info("Unable to convert file contents of: " + filename + " to byte array");
        }
        return new byte[1]; // return byte array of size 1, if unable to read file content
    }

    public boolean put(String msg) {
        boolean putStatus = false;
        String type = msgTypes.get(2);
        //if it is a file , path will contain the whole file path including the extension
        String path = msg;
        // payload contains file contents, in case it is a file
        String payload = new String(convertFileContent(msg));
        Route r = sendMessageToServer(type, path, payload);
        if (new String(r.getPayload().toByteArray()).equalsIgnoreCase("success")) {
            putStatus = true;
            logger.info("Successfully saved: " + msg);
        } else {
            logger.info("Could not save: " + msg);
        }
        return putStatus;
    }

    public boolean checkIfFile(String msg) {
        try {
            RandomAccessFile f = new RandomAccessFile(msg, "r");
        }
        catch (FileNotFoundException fe) {
            logger.info("Not a file");
            return false;
        }
        return true;
    }

    public boolean delete(String msg) {
        boolean deleteStatus = false;
        String type = msgTypes.get(4);
        String path = msg;
        String payload = msg;
        Route r = sendMessageToServer(type, path, payload);
        if (new String(r.getPayload().toByteArray()).equalsIgnoreCase("success")) {
            deleteStatus = true;
            logger.info("Successfully deleted: " + msg);
        } else {
            logger.info("Could not delete: " + msg);
        }
        return deleteStatus;
    }

    public byte[] get(String msg) {
        String type = msgTypes.get(1);
        String path = msg;
        String payload = msg;
        Route r = sendMessageToServer(type, path, payload);
        logger.info("Retrieved information about: "+msg+"  from slaves");
        return r.getPayload().toByteArray();
    }

    public List<String> list() {
        String type = msgTypes.get(3);
        String path = "/list/messages";
        String payload = "listing";
        Route r = sendMessageToServer(type, path, payload);
        payload = r.getPayload().toString();
        logger.info("Received a list of messages or files for the client: "+name+" from slaves");
        return new ArrayList<>(Arrays.asList(payload.split(",")));
    }
}

 /*public static boolean sampleBlocking(String msg, String type) {
        boolean status = false;

        Route.Builder bld = Route.newBuilder();
        //bld.setId(i);
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath(type + "/to/server");
        bld.setType(type);
        byte[] hello = msg.getBytes();
        bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = RouteClient.stub.request(bld.build());

        // TODO response handling
        String payload = new String(r.getPayload().toByteArray());
        System.out.println("reply: " + payload + ", from: " + r.getOrigin());
        if (payload.equalsIgnoreCase("success")) {
            status = true;
        }

        return status;

    }

    public static boolean deleteMessage(String msg) {
        String type = "message-delete";
        boolean result = false;
        Route.Builder bld = Route.newBuilder();
        //bld.setId(1);
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath(type + "/to/server");
        bld.setType(type);
        byte[] hello = msg.getBytes();
        bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = RouteClient.stub.request(bld.build());

        // TODO response handling
        String payload = new String(r.getPayload().toByteArray());
        System.out.println("reply: " + payload + ", from: " + r.getOrigin());
        if (payload.equalsIgnoreCase("success")) {
            result = true;
        }
        return result;
    }

    public static String getSavedMessage(String msg, String type) {
        Route.Builder bld = Route.newBuilder();
        //bld.setId(1);
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath(type + "/to/server");
        bld.setType(type);
        byte[] hello = msg.getBytes();
        bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = RouteClient.stub.request(bld.build());

        // TODO response handling
        String payload = new String(r.getPayload().toByteArray());
        System.out.println("reply: " + payload + ", from: " + r.getOrigin());
        return payload;
    }

    private static void sampleStreaming(String filename, String type) {
        if (filename == null)
            return;

        // NOTE filename is not used in the example, see server
        // implementation for details.

        System.out.println("Streaming: " + filename);

        Route.Builder bld = Route.newBuilder();
        //bld.setId(0);
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath("/save/file");

        bld.setType(type);
        byte[] fn = filename.getBytes();

        try {
            RandomAccessFile f = new RandomAccessFile(filename, "r");
            byte[] b = new byte[(int) f.length()];
            f.readFully(b);
            f.close();
            bld.setPayload(ByteString.copyFrom(b));
        } catch (IOException ie) {
            System.out.println("Unable to copy data from file..");
        }

        // we are still blocking!
        Route r = RouteClient.stub.request(bld.build());
        //while (rIter.hasNext()) {
        // process responses
        //Route r = rIter.next();
        String payload = new String(r.getPayload().toByteArray());
        System.out.println("reply: " + payload + ", from: " + r.getOrigin());
        System.out.println("reply payload: " + new String(r.getPayload().toByteArray()));
    }


    public byte[] getSavedFile(String filename, String type) {
        Route.Builder bld = Route.newBuilder();
        //bld.setId(1);
        bld.setOrigin(origin);
        bld.setPath(type + "/to/server");
        bld.setDestination(destination);
        bld.setType(type);
        byte[] hello = filename.getBytes();
        bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = RouteClient.stub.request(bld.build());

        // TODO response handling
        String payload = new String(r.getPayload().toByteArray());
        System.out.println("reply: " + payload + ", from: " + r.getOrigin());
        return payload.getBytes();
    }

    public boolean putString(String msg) {
        String type = "message-put";
        return sampleBlocking(msg, type);
    }


    public String getString(String msg) {
        String type = "message-get";
        return getSavedMessage(msg, type);
    }


    public List<String> listMessages() {
        //List<String> stringList = new ArrayList<>();
        String type = "message-list";
        Route.Builder bld = Route.newBuilder();
        //bld.setId(1);
        bld.setOrigin(origin);
        bld.setDestination(destination);
        bld.setPath(type + "/to/server");
        bld.setType(type);
        byte[] hello = name.getBytes();
        //bld.setPayload(ByteString.copyFrom(hello));

        // blocking!
        Route r = RouteClient.stub.request(bld.build());

        // TODO response handling
        String payload = new String(r.getPayload().toByteArray());
        System.out.println("reply: " + payload + ", from: " + r.getOrigin());
        List<String> myList = new ArrayList<String>(Arrays.asList(payload.split(",")));
        return myList;
    }

    public boolean putFile(String filepath) {
        //TODO: store the file in the given path

        // 1. validate the given file path
        File fn = new File(filepath);

        // 2. send the file to server (call sendMessage())
        if (fn.exists()) {
            sampleStreaming(filepath, "file-put");
        } else {
            System.out.println("File does not exist in the given path");
            return false;
        }

        // 3. if success from server, return true
        return true;
    }

    public byte[] getFile(String filename) {
        //TODO: retrieve the file with given name
        // 1. send the file name request to server
        return getSavedFile(filename, "file-get");
        // 2. wait for response from server
        // 3. return the file content
        //return new byte[5];
    }

    public List<String> listFiles() {
        // TODO: list all the files stored
        // 1. send the list file request to server

        // 2. wait for response from server
        // 3. return the file list
        return new ArrayList<>();
    }

    public boolean deleteFile(String filename) {
        //TODO: delete the file with given name
        // 1. send the delete file name request to server
        // 2. wait for response from server
        // 3. return success on deletion of the file
        return true;
    }


}
*/
