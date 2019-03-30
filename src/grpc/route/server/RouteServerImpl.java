package grpc.route.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.lang.*;
import java.util.concurrent.CountDownLatch;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lease.Dhcp_Lease_Test;
import main.db.MongoDBHandler;
import main.db.RedisHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import route.RouteServiceGrpc;
import utility.FetchConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc.RouteServiceImplBase;


public class RouteServerImpl extends RouteServiceImplBase {
    protected static Logger logger = LoggerFactory.getLogger("server");
    private Server svr;
    private String name;
    private static boolean isMaster = false;
    private static String myIp = "server";
    private static List<String> slaveips = new ArrayList<>();
    private static Dhcp_Lease_Test dhcp_lease_test = new Dhcp_Lease_Test();
    static MongoDBHandler mh = new MongoDBHandler();
    static RedisHandler rh = new RedisHandler();
    private ManagedChannel ch;
    private RouteServiceGrpc.RouteServiceStub stub;
    private Route response = Route.newBuilder().build();
    private String clientIp = "localhost";

    /**
     * TODO refactor this!
     *
     * @return
     */


    private String join(Route r) {
        //save client user name
        name = r.getUsername();
        logger.info("--> join: " + name);
        myIp = r.getDestination();
        MasterNode.setMasterIp(myIp);
        MasterNode.setUsername(name);
        //TODO: run a background theread continuously monitoring slave ip list
        getSlaveIpList();
        return "WELCOME";
    }

    private String put(Route r) {
        logger.info("--> Message from: " + name + " asking to save: " + r.getPath());
        MasterNode.put(r);
        return "success";
    }

    private String list(Route r) {
        logger.info("--> Message from: " + name + " asking to list all messages or files");
        return MasterNode.list(r);
    }

    private String delete(Route r) {
        String message = new String(r.getPayload().toByteArray());
        logger.info("--> Message from: " + name + " asking to delete: " + message);
        if (MasterNode.delete(r)) {
            return "success";
        }
        return "failure";
    }

    private String requestIp() {
        logger.info("--> Message from: " + name + " asking to assign ip");
        return MasterNode.sendIpToNode(dhcp_lease_test.getCurrentNodeMapping(), dhcp_lease_test.getCurrentIpList());
    }

    private String nodeInfo(Route r) {
        logger.info("--> Message from: " + name + " asking to save client information");
        clientIp = r.getOrigin();
        if (dhcp_lease_test.updateCurrentNodeMapping(r, r.getOrigin())) {
            return "success";
        }
        return "failure";
    }

    private String complete(Route r) {
        logger.info("--> Message from client, saying streaming of file is completed");
        if (MasterNode.complete(r)) {
            return "success";
        }
        return "failure";
    }


    protected ByteString processMaster(route.Route msg) {
        String reply;
        logger.info("master: Message type: " + msg.getType());
        if (msg.getType().equalsIgnoreCase("join")) {
            reply = join(msg);

        } else if (msg.getType().equalsIgnoreCase("put")) {
            reply = put(msg);

        } else if (msg.getType().equalsIgnoreCase("list")) {
            reply = list(msg);

        } else if (msg.getType().equalsIgnoreCase("delete")) {
            reply = delete(msg);

        } else if (msg.getType().equalsIgnoreCase("request-ip")) {
            reply = requestIp();

        } else if (msg.getType().equalsIgnoreCase("save-client-ip")) {
            reply = nodeInfo(msg);

        } else if (msg.getType().equalsIgnoreCase("put-complete")) {
            reply = complete(msg);
        } else {
            logger.info("--> Got data from: " + msg.getOrigin() + "  path: " + msg.getPath());
            reply = "no-type";
        }

        byte[] raw = reply.getBytes();
        return ByteString.copyFrom(raw);
    }


    public void getSlaveIpList() {
        Map<String, List<String>> map = dhcp_lease_test.getCurrentNodeMapping();
        if (map.containsKey("slave")) {
            slaveips = map.get("slave");
        }
        MasterNode.assignSlaveIp(slaveips);
    }

    private String listSlave(Route r) {
        String actualmessage = new String(r.getPayload().toByteArray());
        logger.info("--> Message from: master asking to list messages or files of: " + r.getUsername());
        //TODO: implement list
        //           reply = SlaveNode.list(r).toString();
        return new String();
    }

    private String deleteSlave(Route r) {
        String actualmessage = new String(r.getPayload().toByteArray());
        logger.info("--> Message from: master asking to delete: " + actualmessage);
        if (SlaveNode.delete(r)) {
            logger.info("success-slave-delete");
            return "success";
        }
        logger.info("failure-slave-delete");
        return "failure";
    }

    private String slaveIp(Route r) {
        logger.info("Got a message from master of type: " + r.getType() + " with payload: " + r.getPayload());
        myIp = new String(r.getPayload().toByteArray());
        logger.info("Assigned ip: " + myIp + " by DHCP server node");
        return "slave";
    }


    protected ByteString processSlave(route.Route msg) {
        logger.info("Slave: Processing msg of type: " + msg.getType() + "with: " + msg.getPayload());
        name = msg.getUsername();
        String reply = null;
        if (msg.getType().equalsIgnoreCase("list")) {
            reply = listSlave(msg);

        } else if (msg.getType().equalsIgnoreCase("delete")) {
            reply = deleteSlave(msg);

        } else if (msg.getType().equalsIgnoreCase("slave-ip")) {
            reply = slaveIp(msg);

        } else {
            String content = new String(msg.getPayload().toByteArray());
            logger.info("Got content: " + content + " from: " + msg.getOrigin() + " path: " + msg.getPath());
            reply = "no-type";
        }
        if (reply == null) {
            reply = "";
        }
        byte[] raw = reply.getBytes();
        return ByteString.copyFrom(raw);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            logger.info("Missing server configuration");
            return;
        }
        String path = args[0];
        Properties conf = FetchConfig.getConfiguration(new File(path));
        RouteServer.configure(conf);

        final RouteServerImpl impl = new RouteServerImpl();
        if (conf.getProperty("server.name").equalsIgnoreCase("master")) {
            isMaster = true;
            logger.info("Running as Master node");
        } else {
            logger.info("Running as Slave node");
        }
        impl.start();
        impl.blockUntilShutdown();
    }

    private void invokeDhcpMonitorThread() {
        Thread thread = new Thread() {
            public void run() {
                logger.info("Starting DHCP Lease Monitor Thread...");
                dhcp_lease_test.monitorLease();
            }
        };
        thread.start();
    }

    private void start() throws Exception {
        svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteServerImpl())
                .build();

        logger.info("Starting server..");
        svr.start();
        if (isMaster) {
            invokeDhcpMonitorThread();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                RouteServerImpl.this.stop();
            }
        });
    }

    protected void stop() {
        svr.shutdown();
    }

    private void blockUntilShutdown() throws Exception {
        svr.awaitTermination();
    }

    public Route collectStreamingDataInSlave(Route r) {
        logger.info("Saving chunk with seq num: " + r.getSeq() + " in slave");
        boolean putStatus = SlaveNode.put(r);
        if (putStatus) {
            logger.info("Successfully saved chunk in slave");
        }

        ch = ManagedChannelBuilder.forAddress(r.getOrigin(), Integer.parseInt("2345".trim())).usePlaintext(true).build();
        stub = RouteServiceGrpc.newStub(ch);


        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<Route> requestObserver = stub.request(new StreamObserver<Route>() {
            @Override
            public void onNext(Route route) {
                logger.info("collectStreamingDataInSlave:received response from master: " + new String(route.getPayload().toByteArray()));
                response = route;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("collectStreamingDataInSlave: Exception in the response from master while: " + throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("collectStreamingDataInSlave: Master is done sending data");
                latch.countDown();
            }
        });

        Route.Builder bld = Route.newBuilder();
        bld.setUsername(r.getUsername());
        bld.setOrigin(myIp);
        bld.setDestination(r.getOrigin());
        String status = "failure";
        if (putStatus) {
            status = "success";
        }
        bld.setPayload(ByteString.copyFrom(status.getBytes()));
        logger.info("sending response to master: " + status);
        bld.setType(r.getType());
        logger.info("request type is: " + bld.getType());
        bld.setPath(bld.getPath());
        bld.setSeq(bld.getSeq());
        requestObserver.onNext(bld.build());
        requestObserver.onCompleted();
        return bld.build();
    }


    public void sendDataToMasterInChunks(Route r) {
        logger.info("Request from master, asking to retrieve: " + r.getPath());
        SlaveNode.returnFileInchunks(r);
    }

    //respond to a request
    @Override
    public void blockingrequest(route.Route request, StreamObserver<route.Route> responseObserver) {
        route.Route.Builder builder = route.Route.newBuilder();
        builder.setPath(request.getPath());

        // do the work
        if (isMaster) {
            builder.setPayload(processMaster(request));
            if (myIp != null) {
                builder.setOrigin(myIp);
            }
            builder.setDestination(request.getOrigin());
        } else {
            builder.setPayload(processSlave(request));
            builder.setOrigin(myIp);
            builder.setDestination(request.getOrigin());
        }
        route.Route rtn = builder.build();
        responseObserver.onNext(rtn);
        responseObserver.onCompleted();
    }

    private Route completeResponse(Route route) {
        Route.Builder bld = Route.newBuilder();
        bld.setUsername(route.getUsername());
        bld.setOrigin(myIp);
        bld.setDestination(route.getOrigin());
        bld.setPayload(ByteString.copyFrom("success".getBytes()));
        bld.setType(route.getType());
        logger.info("request type is: " + bld.getType());
        bld.setPath(route.getPath());
        bld.setSeq(route.getSeq());
        return bld.build();
    }

    private Route handleComplete(Route route) {
        if (isMaster) {
            logger.info("origin: " + route.getOrigin());
            logger.info("Received complete message from client-master");
            Route.Builder bld = Route.newBuilder();
            bld.setUsername(route.getUsername());
            bld.setOrigin(myIp);
            bld.setDestination(route.getOrigin());
            bld.setPayload(processMaster(route));
            bld.setType(route.getType());
            bld.setSeq(route.getSeq());
            return bld.build();
        } else {
            logger.info("Entering complete- slave");
            SlaveNode.put(route.getUsername(), route.getPath());
            return completeResponse(route);
        }
    }

    private Route handlePut(Route route) {
        if (isMaster) {
            logger.info("Receiving file data with seq num: " + route.getSeq() + " from: " + name);
            ByteString msg = processMaster(route);
            logger.info("Received complete message from client-master");
            Route.Builder bld = Route.newBuilder();
            bld.setUsername(route.getUsername());
            bld.setOrigin(myIp);
            bld.setDestination(route.getOrigin());
            bld.setPayload(msg);
            bld.setType(route.getType());
            bld.setSeq(route.getSeq());
            logger.info("put: received response from slave: " + new String(msg.toByteArray()));
            return bld.build();
        } else {
            return collectStreamingDataInSlave(route);
        }
    }

    private void handleSlaveIp(Route route) {
        Route.Builder builder = Route.newBuilder();
        builder.setPayload(processSlave(route));
        builder.setOrigin(myIp);
        builder.setDestination(route.getOrigin());
        builder.setPath(route.getPath());
        builder.setUsername(route.getUsername());
        builder.setSeq(route.getSeq());
        ManagedChannel ch = ManagedChannelBuilder.forAddress(route.getOrigin(), Integer.parseInt("2345".trim())).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceBlockingStub blockingStub = RouteServiceGrpc.newBlockingStub(ch);
        Route r = blockingStub.blockingrequest(builder.build());
        logger.info("--got: " + new String(r.getPayload().toByteArray()) + " from: " + r.getOrigin());
    }

    private void handleGet(Route r) {
        ManagedChannel ch = ManagedChannelBuilder.forAddress(clientIp, Integer.parseInt("2345".trim())).usePlaintext(true).build();
        RouteServiceGrpc.RouteServiceStub stub = RouteServiceGrpc.newStub(ch);
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<Route> requestObserver = stub.request(new StreamObserver<Route>() {
            @Override
            public void onNext(Route route) {
                logger.info("handleGet:received response from client: " + new String(route.getPayload().toByteArray()));
                response = route;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("√: Exception in the response from client while: " + throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("handleGet: client is done sending data");
                latch.countDown();
            }
        });

        Route.Builder builder = Route.newBuilder();
        builder.setPayload(r.getPayload());
        builder.setOrigin(myIp);
        builder.setDestination(clientIp);
        builder.setPath(r.getPath());
        builder.setUsername(r.getUsername());
        builder.setSeq(r.getSeq());
        logger.info("sending seq no: " + r.getSeq());
    }


    @Override
    public StreamObserver<Route> request(StreamObserver<route.Route> responseObserver) {
        StreamObserver<Route> requestObserver = new StreamObserver<Route>() {
            String userName;
            String filePath;
            String methodType;
            ByteString payload;

            //handle requests from client here
            @Override
            public void onNext(Route route) {
                logger.info("on next called");
                userName = route.getUsername();
                filePath = route.getPath();
                methodType = route.getType();
                payload = route.getPayload();

                route.Route.Builder builder = Route.newBuilder();
                builder.setPath(route.getPath());

                if (methodType.equalsIgnoreCase("put-complete")) {
                    logger.info("in put-complete");
                    responseObserver.onNext(handleComplete(route));
                } else if (route.getType().equalsIgnoreCase("put")) {
                    responseObserver.onNext(handlePut(route));

                } else if (route.getType().equalsIgnoreCase("get-complete")) {
                    responseObserver.onNext(route);
                } else if (route.getType().equalsIgnoreCase("get")) {
                    if (isMaster) {
                        logger.info("entering master get");
                        Route r = MasterNode.collectDataFromSlaves(route);
                        logger.info("received response");
                        responseObserver.onNext(r);
                        logger.info("sent data to client");
                    } else {
                        logger.info("entering slave get");
                        SlaveNode.returnFileInchunks(route);
                    }

                } else if (methodType.equalsIgnoreCase("get-complete")) {
                    responseObserver.onNext(route);
                } else {
                    if (isMaster) {
                        builder.setPayload(processMaster(route));
                        builder.setOrigin(myIp);
                        builder.setDestination(route.getOrigin());
                        responseObserver.onNext(builder.build());

                    } else {
                        if (methodType.equalsIgnoreCase("slave-ip")) {
                            handleSlaveIp(route);
                        } else {
                            builder.setPayload(processSlave(route));
                            builder.setOrigin(myIp);
                            builder.setDestination(route.getOrigin());
                            route.Route rtn = builder.build();
                            logger.info("sending reply with payload: " + new String(builder.getPayload().toByteArray()));
                            responseObserver.onNext(rtn);
                        }
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the request from node: " + throwable);
            }

            @Override
            public void onCompleted() {
                logger.info("Node is done sending messages");
                responseObserver.onCompleted();
            }
        };
        return requestObserver;
    }
}