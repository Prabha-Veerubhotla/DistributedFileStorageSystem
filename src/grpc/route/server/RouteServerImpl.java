package grpc.route.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.lang.*;

import com.google.protobuf.ByteString;
import lease.Dhcp_Lease_Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
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
    private List<String> msgTypes = FetchConfig.getMsgTypes();

    /**
     * TODO refactor this!
     *
     * @param path
     * @param payload
     * @return
     */

    protected ByteString processMaster(route.Route msg) {

        String reply;

        if (msg.getType().equalsIgnoreCase(msgTypes.get(0))) {
            //save client user name
            name = msg.getUsername();
            logger.info("--> join: " + name);
            reply = "welcome";
            myIp = msg.getDestination();
            MasterNode.setMasterIp(myIp);
            MasterNode.setUsername(name);
            //TODO: run a background theread continuously monitoring slave ip list
            getSlaveIpList();

        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(1))) {
            String message = new String(msg.getPayload().toByteArray());
            logger.info("--> message from: " + name + " asking to retrieve: " + message);
            reply = MasterNode.get(msg).toString();

        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(2))) {
            String message = new String(msg.getPayload().toByteArray());
            logger.info("--> message from: " + name + " asking to save: " + msg.getPath());
            if (MasterNode.put(msg)) {
                reply = "success";
            } else {
                reply = "failure";
            }

        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(3))) {
            logger.info("--> message from: " + name + " asking to list all messages or files");
            reply = MasterNode.list(msg);

        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(4))) {
            String message = new String(msg.getPayload().toByteArray());
            logger.info("--> message from: " + name + " asking to delete: " + message);
            if (MasterNode.delete(msg)) {
                reply = "success";
            } else {
                reply = "failure";
            }
        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(5))) {
            logger.info("--> message from: " + name + " asking to assign ip");
            reply = MasterNode.sendIpToNode(dhcp_lease_test.getCurrentNodeMapping(), dhcp_lease_test.getCurrentIpList());
        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(6))) {
            logger.info("--> message from: " + name + " asking to save client information");
            if (dhcp_lease_test.updateCurrentNodeMapping(msg, msg.getOrigin())) {
                reply = "success";
            } else {
                reply = "failure";
            }

        } else {
            // TODO placeholder
            String content = new String(msg.getPayload().toByteArray());
            logger.info("-- got: " + content + "from: " + msg.getOrigin() + "  path: " + msg.getPath());
            reply = "blank";
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


    protected ByteString processSlave(route.Route msg) {

        name = msg.getUsername();

        String reply;

        if (msg.getType().equalsIgnoreCase(msgTypes.get(1))) {
            String actualmessage = new String(msg.getPayload().toByteArray());
            logger.info("--> message from: master asking to retrieve: " + actualmessage);
            reply = SlaveNode.get(msg).toString();

        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(2))) {
            String actualmessage = new String(msg.getPayload().toByteArray());
            logger.info("--> message from: master asking to save: " + msg.getPath());
            logger.info("received message from master asking to save seq num: " + msg.getSeq());
            if (SlaveNode.put(msg)) {
                logger.info("--saved message: " + actualmessage + " from: " + name + " successfully");
                reply = "success";
            } else {
                reply = "failure";
                logger.info("--unable to save message: " + actualmessage + "from: " + name);
            }
        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(3))) {
            String actualmessage = new String(msg.getPayload().toByteArray());
            logger.info("--> message from: master asking to list messages or files of: " + msg.getUsername());
            reply = SlaveNode.list(msg).toString();
        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(4))) {
            String actualmessage = new String(msg.getPayload().toByteArray());
            logger.info("--> message from: master asking to delete: " + actualmessage);
            if (SlaveNode.delete(msg)) {
                reply = "success";
            } else {
                reply = "failure";
            }
        } else if (msg.getType().equalsIgnoreCase(msgTypes.get(7))) {
            String actualmessage = new String(msg.getPayload().toByteArray());
            logger.info("assigned ip: " + myIp + " by dhcp server node");
            myIp = actualmessage;
            reply = "slave";
        } else {

            // TODO placeholder
            String content = new String(msg.getPayload().toByteArray());
            logger.info(" got: " + content + "from: " + msg.getOrigin() + " path: " + msg.getPath());
            reply = "blank";
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
            logger.info("Running as master node");
        } else {
            logger.info("Running as slave node");
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

        logger.info("-- starting server");
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

    public void sendStreamingDataToSlave(Route r) {
        logger.info("sending each chunk to slave");
        MasterNode.sendMessageToSlaves(r);

    }

    public void collectStreamingDataInSlave(Route r) {
        logger.info("saving each chunk in slave");
        //receiving each chunk in slave and writing into a file
        SlaveNode.writeChunksIntoFile(r);
    }

    public Route collectDataFromSlavesInChunks(Route r) {
        return MasterNode.collectDataFromSlaves(r);
    }

    public void sendDataToMasterInChunks(Route r) {
        SlaveNode.returnFileInchunks(r);
    }

    @Override
    public StreamObserver<Route> request(StreamObserver<route.Route> responseObserver) {
        StreamObserver<Route> requestObserver = new StreamObserver<Route>() {

            //handle requests from client here
            @Override
            public void onNext(Route route) {
                logger.info("received file data with seq num: " + route.getSeq());
                route.Route.Builder builder = Route.newBuilder();
                builder.setPath(route.getPath());

                if (route.getType().equalsIgnoreCase("put")) {
                    if (isMaster) {
                        sendStreamingDataToSlave(route);
                    } else {
                        collectStreamingDataInSlave(route);
                    }
                   /* builder.setType(route.getType());
                    builder.setOrigin(myIp);
                    builder.setDestination(route.getOrigin());
                    builder.setSeq(route.getSeq());
                    builder.setPayload(ByteString.copyFrom("success".getBytes()));
                    responseObserver.onNext(route);*/
                }
                if (route.getType().equalsIgnoreCase("get")) {
                    if (isMaster) {
                        collectDataFromSlavesInChunks(route);
                    } else {
                        sendStreamingDataToSlave(route);
                    }
                    responseObserver.onNext(route);
                } else {
                    if (isMaster) {
                        builder.setPayload(processMaster(route));
                        builder.setOrigin(myIp);
                        builder.setDestination(route.getOrigin());
                    } else {
                        builder.setPayload(processSlave(route));
                        builder.setOrigin(myIp);
                        builder.setDestination(route.getOrigin());
                    }
                    route.Route rtn = builder.build();
                    responseObserver.onNext(rtn);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the request from client: " + throwable);
            }

            @Override
            public void onCompleted() {
                logger.info("completed sending messages to client");
                responseObserver.onCompleted();
            }
        };
        return requestObserver;
    }
}

