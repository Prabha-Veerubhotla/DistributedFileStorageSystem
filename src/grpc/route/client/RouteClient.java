package grpc.route.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import route.RouteServiceGrpc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    private static RouteServiceGrpc.RouteServiceStub stub;
    private Properties setup;
    private String name;
    private static String myIp = "client"; // intially , later master node will assign an ip
    protected static Logger logger = LoggerFactory.getLogger("client");
    private List<String> msgTypes = new ArrayList<>();
    private Route response = Route.newBuilder().setDestination("server").build();

    public RouteClient(Properties setup) {
        this.setup = setup;
    }

    public void setName(String clientName) {
        name = clientName;
        logger.info("setting client name as: " + name);
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
        stub = RouteServiceGrpc.newStub(ch);
        System.out.println("Client running...");
        msgTypes = FetchConfig.getMsgTypes();
        //request ip from node running dhcp-server
        // requestIp();
        //reply node info stating that you are client
        //sendNodeInfo();
    }

    public boolean checkIfFile(String msg) {
        try {
            RandomAccessFile f = new RandomAccessFile(msg, "r");
        } catch (FileNotFoundException fe) {
            logger.info("Not a file");
            return false;
        }
        return true;
    }

    private void sendMessageToServer(String type, String path, String payload) {
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<Route> requestObserver = stub.request(new StreamObserver<Route>() {
            //handle response from server here
            @Override
            public void onNext(Route route) {
                /*if (route.getType().equalsIgnoreCase("put")) {
                    synchronized (response) {
                        try {
                            response.wait();
                            response = route.toBuilder().build();
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    }
                } else*/ if (route.getType().equalsIgnoreCase("get")) {

                    File file = new File("output-" + route.getPath());
                    //Create the file
                    try {
                        if (file.createNewFile()) {
                            System.out.println("File is created!");
                        } else {
                            System.out.println("File already exists.");
                        }
                        RandomAccessFile f = new RandomAccessFile(file, "rw");
                        f.write(route.getPayload().toByteArray(), 0, 1);
                        f.close();
                    } catch (IOException io) {
                        io.printStackTrace();
                    }
                    synchronized (response) {
                        try {
                            response.wait();
                            response = route.toBuilder().build();
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    }
                } else {
                    response = route.toBuilder().build();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the response from server: " + throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Server is done sending data");
                latch.countDown();
                /*synchronized (response) {
                    response.notifyAll();
                }*/
            }
        });

        logger.info("sending request to server of type: " + type);
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(myIp);
        bld.setDestination(setup.getProperty("host")); // from the args , when we start client
        bld.setType(type);
        bld.setUsername(name);
        bld.setPath(path);
        // if msg is post, if it is a file, stream it
        if (type.equalsIgnoreCase(msgTypes.get(2))) {
            if (payload == null)
                return;
            if (checkIfFile(payload)) {
                System.out.println("This is a file");
                File fn = new File(payload);
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(fn);
                    long seq = 0l;
                    final int blen = 10024;
                    byte[] raw = new byte[blen];
                    boolean done = false;
                    while (!done) {
                        int n = fis.read(raw, 0, blen);
                        if (n <= 0)
                            break;
                        System.out.println("n: " + n);
                        // identifying sequence number
                        seq++;
                        bld.setPayload(ByteString.copyFrom(raw, 0, n));
                        bld.setSeq(seq);
                        logger.info("Sending file data to server with seq num: " + seq);
                        // convert string to byte string,
                        // to be compatible with protobuf format

                        // blocking!

                        requestObserver.onNext(bld.build());


                    }
                } catch (IOException e) {
                    ; // ignore? really?
                    requestObserver.onError(e);
                } finally {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        ; // ignore
                    }
                }
            } else {
                bld.setPayload(ByteString.copyFrom(payload.getBytes()));
                logger.info("Sending request to server with payload: " + payload);
                requestObserver.onNext(bld.build());

            }
        } else {
            bld.setPayload(ByteString.copyFrom(payload.getBytes()));
            logger.info("Sending request to server with payload: " + payload);
            requestObserver.onNext(bld.build());

        }
        requestObserver.onCompleted();
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Exception while waiting for count down latch: " + ie);
        }


    }

    public boolean join() {
        String type = msgTypes.get(0);
        String payload = "joining";
        String path = "/client/joining";
        sendMessageToServer(type, path, payload);
        //TODO: use some kind of wait, notify
        logger.info("reply from master node: " + new String(response.getPayload().toByteArray()));
        if (new String(response.getPayload().toByteArray()).equalsIgnoreCase("welcome")) {
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
        sendMessageToServer(type, path, payload);
        myIp = new String(response.getPayload().toByteArray());
    }

    public void sendNodeInfo() {
        String type = msgTypes.get(6);
        String path = "sending/node/info";
        String payload = "client";
        sendMessageToServer(type, path, payload);
        if (new String(response.getPayload().toByteArray()).equalsIgnoreCase("success")) {
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
            logger.info("file length is: " + f.length());
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
        String type = msgTypes.get(2);
        String path = msg;
        String payload = msg;
        boolean putStatus = false;
        System.out.println("Streaming: " + msg);
        sendMessageToServer(type, path, payload);
       // synchronized (response) {
          //  try {
                //response.wait();
                if (new String(response.getPayload().toByteArray()).equalsIgnoreCase("success")) {
                    putStatus = true;
                    logger.info("Successfully saved: " + msg);
                } else {
                    logger.info("Could not save: " + msg);
                }
            /*} catch (InterruptedException ie) {
                ie.printStackTrace();
            }*/
        
        return putStatus;
    }


    public boolean delete(String msg) {
        boolean deleteStatus = false;
        String type = msgTypes.get(4);
        String path = msg;
        String payload = msg;
        sendMessageToServer(type, path, payload);
        if (new String(response.getPayload().toByteArray()).equalsIgnoreCase("success")) {
            deleteStatus = true;
            logger.info("Successfully deleted: " + msg);
        } else {
            logger.info("Could not delete: " + msg);
        }
        return deleteStatus;
    }

    public File get(String msg) {
        String type = msgTypes.get(1);
        String path = msg;
        String payload = msg;
        sendMessageToServer(type, path, payload);
        logger.info("Retrieved information about: " + msg + "  from slaves");
        synchronized (response) {
            try {
                response.wait();
            } catch(InterruptedException ie) {
                ie.printStackTrace();
            }
        }
        return new File("output-"+msg);
    }

    public List<String> list() {
        String type = msgTypes.get(3);
        String path = "/list/messages";
        String payload = "listing";
        sendMessageToServer(type, path, payload);
        payload = response.getPayload().toString();
        logger.info("Received a list of messages or files for the client: " + name + " from slaves");
        return new ArrayList<>(Arrays.asList(payload.split(",")));
    }
}

