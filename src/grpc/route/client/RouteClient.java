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

//TODO: make get, put , list, update asynchronous
//TODO: make the rem, calls blocking -- done
//TODO: listen continuously for messages from server on a background thread

public class RouteClient {
    private static ManagedChannel ch;
    private static RouteServiceGrpc.RouteServiceStub stub;
    private static RouteServiceGrpc.RouteServiceBlockingStub blockingStub;
    private Properties setup;
    private String name;
    private static String myIp = "client"; // intially , later master node will assign an ip
    protected static Logger logger = LoggerFactory.getLogger("client");
    private Route response = Route.newBuilder().build();

    public RouteClient(Properties setup) {
        this.setup = setup;
    }

    public void setName(String clientName) {
        name = clientName;
        logger.info("Setting client name as: " + name);
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
        //TODO: make it async stub -- done
        stub = RouteServiceGrpc.newStub(ch);
        blockingStub = RouteServiceGrpc.newBlockingStub(ch);
        System.out.println("Client running...");
        //request ip from node running dhcp-server
        requestIp(); // blocking
        //reply node info stating that you are client
        sendNodeInfo(); //blocking
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

    private route.Route buildError(route.Route request, String msg) {
        route.Route.Builder builder = route.Route.newBuilder();
        builder.setOrigin(myIp);
        builder.setDestination(request.getOrigin());
        builder.setPath(request.getPath());

        // do the work
        logger.info("--> Got data from: " + request.getOrigin() + "  content: " + request.getPayload());
        String reply = "blank";
        byte[] raw = reply.getBytes();
        builder.setPayload(ByteString.copyFrom(raw));
        route.Route rtn = builder.build();

        return rtn;
    }

    private void sendMessageToServer(String type, String path, String payload) {
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<Route> requestObserver = stub.request(new StreamObserver<Route>() {
            //handle response from server here
            @Override
            public void onNext(Route route) {
                response = route.toBuilder().build();
                logger.info("received response from server: " + new String(response.getPayload().toByteArray()));
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
            }
        });

        logger.info("Sending request to server of type: " + type);
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(myIp);
        bld.setDestination(setup.getProperty("host")); // from the args , when we start client
        bld.setType(type);
        bld.setUsername(name);
        bld.setPath(path);

        // if msg is put, if it is a file, stream it
        if (type.equalsIgnoreCase("put")) {
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
                        // identifying sequence number
                        seq++;
                        logger.info("Streaming seq num: " + seq);
                        bld.setPayload(ByteString.copyFrom(raw, 0, n));
                        logger.info("seq num is: " + seq);
                        bld.setSeq(seq);
                        logger.info("Sending file data to server with seq num: " + seq);
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
                logger.info("Streaming file: " + payload + " is done");
                bld.setType("complete");
                bld.setSeq(1);
                bld.setPayload(ByteString.copyFrom("complete".getBytes()));
                logger.info("calling on next-client");
                requestObserver.onNext(bld.build());
                logger.info("Sending complete message to master");

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
        logger.info("calling oncompleted()");
        requestObserver.onCompleted();

        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Exception while waiting for count down latch: " + ie);
        }
    }


    public Route sendBlockingMessageToServer(String type, String path, String payload) {
        Route.Builder bld = Route.newBuilder();
        bld.setOrigin(myIp);
        logger.info("destination is: " + setup.getProperty("host") + " type:" + type + " path:" + path + " payload:" + payload);
        bld.setDestination(setup.getProperty("host")); // from the args , when we start client
        bld.setType(type);
        bld.setUsername(name);
        bld.setPath(path);
        bld.setPayload(ByteString.copyFrom(payload.getBytes()));
        bld.setSeq(0);
        // blocking!
        return RouteClient.blockingStub.blockingrequest(bld.build());
    }

    //blocking
    public boolean join() {
        String type = "join";
        String payload = "joining";
        String path = "/client/joining";
        Route response = sendBlockingMessageToServer(type, path, payload);
        logger.info("reply from master node: " + new String(response.getPayload().toByteArray()));
        if (new String(response.getPayload().toByteArray()).equalsIgnoreCase("welcome")) {
            return true;
        }
        return false;
    }


    public void stopClientSession() {
        ch.shutdown();
    }

    //blocking
    public void requestIp() {
        String type = "request-ip";
        String path = "requesting/client/ip";
        String payload = "/requesting";
        Route response = sendBlockingMessageToServer(type, path, payload);
        myIp = new String(response.getPayload().toByteArray());
        logger.info("my ip is: " + myIp);
    }

    //blocking
    public void sendNodeInfo() {
        String type = "save-client-ip";
        String path = "sending/node/info";
        String payload = "client";
        Route response = sendBlockingMessageToServer(type, path, payload);
        if (new String(response.getPayload().toByteArray()).equalsIgnoreCase("success")) {
            logger.info("Got node information from master node");
            logger.info("My IP is: " + myIp);
        }
    }

    //non blocking
    public boolean put(String msg) {
        String type = "put";
        String path = msg;
        String payload = msg;
        boolean putStatus = false;
        System.out.println("Streaming: " + msg);
        sendMessageToServer(type, path, payload);

        if (new String(response.getPayload().toByteArray()).equalsIgnoreCase("success")) {
            putStatus = true;
            logger.info("Successfully saved: " + msg);
        } else {
            logger.info("Could not save: " + msg);
        }

        return putStatus;
}


    //blocking
    public boolean delete(String msg) {
        boolean deleteStatus = false;
        String type = "delete";
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

    //non blocking
    public File get(String msg) {
        String type = "get";
        String path = msg;
        String payload = msg;
        sendMessageToServer(type, path, payload);
        logger.info("Retrieved information about: " + msg + "  from slaves");
        synchronized (response) {
            try {
                response.wait();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
        return new File("output-" + msg);
    }

    //blocking
    public List<String> list() {
        String type = "list";
        String path = "/list/messages";
        String payload = "listing";
        sendMessageToServer(type, path, payload);
        payload = response.getPayload().toString();
        logger.info("Received a list of messages or files for the client: " + name + " from slaves");
        return new ArrayList<>(Arrays.asList(payload.split(",")));
    }
}



   /*             if (route.getType().equalsIgnoreCase("get")) {
                        logger.info("Recevied data from master: " + new String(route.getPayload().toByteArray()));
                        File file = new File("output-" + route.getPath());
                        //Create the file
                        try {
                        if (file.createNewFile()) {
                        logger.info("File: " + file + " is created!");
                        } else {
                        logger.info("File: " + file + " already exists.");
                        }
                        RandomAccessFile f = new RandomAccessFile(file, "rw");
                        // write into the file , every chunk received from master
                        f.write(route.getPayload().toByteArray());
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
        }*/

