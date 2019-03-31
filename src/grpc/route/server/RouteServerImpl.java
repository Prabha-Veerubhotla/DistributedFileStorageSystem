package grpc.route.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.lang.*;
import io.grpc.stub.StreamObserver;
import lease.Dhcp_Lease_Test;
import main.db.MongoDBHandler;
import main.db.RedisHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.*;
import utility.FetchConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;


public class RouteServerImpl extends FileServiceGrpc.FileServiceImplBase {
    protected static Logger logger = LoggerFactory.getLogger("server");
    private Server svr;
    private String name;
    private static boolean isMaster = false;
    private static String myIp = "server";
    private static String myPort = "2345";
    private static List<String> slaveips = new ArrayList<>();
    private static Dhcp_Lease_Test dhcp_lease_test = new Dhcp_Lease_Test();
    static MongoDBHandler mh = new MongoDBHandler();
    static RedisHandler rh = new RedisHandler();


    public void getSlaveIpList() {
        Map<String, List<String>> map = dhcp_lease_test.getCurrentNodeMapping();
        if (map.containsKey("slave")) {
            slaveips = map.get("slave");
        }
        MasterNode.assignSlaveIp(slaveips);
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

    private void slaveIpThread() {
        Thread thread = new Thread() {
            public void run() {
                logger.info("Fetching Ip List of Nodes...");
                getSlaveIpList();
                MasterNode.createChannel();
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
            slaveIpThread();
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


    @Override
    public StreamObserver<FileData> uploadFile(StreamObserver<Ack> ackStreamObserver) {
        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;

            @Override
            public void onNext(FileData fileData) {
                fd = fileData;
                username = fileData.getUsername().getUsername();
                filepath = fileData.getFilename().getFilename();
                if (isMaster) {
                    ackStatus = MasterNode.streamFileToServer(fileData, false);
                    if (ackStatus) {
                        ackMessage = "success";
                    } else {
                        ackMessage = "Unable to save file";
                    }
                } else {
                    logger.info("received data from master");
                    ackStatus = SlaveNode.put(fileData);
                    if (ackStatus) {
                        ackMessage = "success";
                    } else {
                        ackMessage = "Unable to save file";
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
                if (isMaster) {
                    if (MasterNode.streamFileToServer(fd, true)) {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("Unable to save file").setSuccess(false).build());
                    }
                    ackStreamObserver.onCompleted();
                } else {
                    if (SlaveNode.put(username, filepath)) {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("Unable to save file in DB").setSuccess(false).build());
                    }
                    ackStreamObserver.onCompleted();
                }
            }
        };
        return fileDataStreamObserver;
    }

    @Override
    public void deleteFile(FileInfo fileInfo, StreamObserver<Ack> ackStreamObserver) {
        Ack.Builder ack = Ack.newBuilder();
        boolean ackStatus;
        String ackMessage = "Unable to save file";
        if (isMaster) {
            ackStatus = MasterNode.deleteFileFromServer(fileInfo);
            if (ackStatus) {
                ackMessage = "success";
            }

        } else {
            ackStatus = SlaveNode.delete(fileInfo);
            if (ackStatus) {
                ackMessage = "success";
            }
        }
        ack.setMessage(ackMessage);
        ack.setSuccess(ackStatus);

        ackStreamObserver.onNext(ack.build());
        ackStreamObserver.onCompleted();
    }

    @Override
    public void searchFile(FileInfo fileInfo, StreamObserver<Ack> ackStreamObserver) {
        Ack.Builder ack = Ack.newBuilder();
        boolean ackStatus;
        String ackMessage = "File is not present";
        if (isMaster) {
            ackStatus = MasterNode.searchFileInServer(fileInfo);
            if (ackStatus) {
                ackMessage = "success";
            }

        } else {
            ackStatus = SlaveNode.search(fileInfo);
            if (ackStatus) {
                ackMessage = "success";
            }
        }
        ack.setMessage(ackMessage);
        ack.setSuccess(ackStatus);

        ackStreamObserver.onNext(ack.build());
        ackStreamObserver.onCompleted();
    }

    @Override
    public void requestNodeIp(NodeName nodeName, StreamObserver<NodeInfo> nodeInfoStreamObserver) {
        NodeInfo.Builder nodeInfo = NodeInfo.newBuilder();
        if (isMaster) {
            nodeInfo.setIp(MasterNode.sendIpToNode(dhcp_lease_test.getCurrentNodeMapping(), dhcp_lease_test.getCurrentIpList()));
            nodeInfo.setPort("2345");
            dhcp_lease_test.updateCurrentNodeMapping(nodeInfo.build(), nodeName);
        }
        logger.info("Requesting to assign ip, for: " + nodeName.getName());
        nodeInfoStreamObserver.onNext(nodeInfo.build());
        nodeInfoStreamObserver.onCompleted();
    }

    @Override
    public void assignNodeIp(NodeInfo nodeInfo, StreamObserver<NodeName> nodeNameStreamObserver) {
        NodeName.Builder nodeName = NodeName.newBuilder();
        if (!isMaster) {
            myIp = nodeInfo.getIp();
            myPort = nodeInfo.getPort();
        }
        logger.info("Assigned ip: " + myIp + "  by DHCP server");
        nodeName.setName("slave");
        nodeNameStreamObserver.onNext(nodeName.build());
        nodeNameStreamObserver.onCompleted();
    }

    @Override
    public void listFile(UserInfo userInfo, StreamObserver<FileResponse> fileResponseStreamObserver) {
        FileResponse.Builder fileResponse = FileResponse.newBuilder();

        if (isMaster) {
            fileResponse.setFilename(MasterNode.listFilesInServer(userInfo));

        } else {
            fileResponse.setFilename(SlaveNode.list(userInfo));

        }

        fileResponseStreamObserver.onNext(fileResponse.build());
        fileResponseStreamObserver.onCompleted();
    }


    @Override
    public StreamObserver<FileData> updateFile(StreamObserver<Ack> ackStreamObserver) {
        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;

            @Override
            public void onNext(FileData fileData) {
                fd = fileData;
                username = fileData.getUsername().getUsername();
                filepath = fileData.getFilename().getFilename();
                if (isMaster) {
                    ackStatus = MasterNode.streamFileToServer(fileData, false);
                    if (ackStatus) {
                        ackMessage = "success";
                    } else {
                        ackMessage = "Unable to update file";
                    }
                } else {
                    logger.info("received data from master");
                    ackStatus = SlaveNode.put(fileData);
                    if (ackStatus) {
                        ackMessage = "success";
                    } else {
                        ackMessage = "Unable to update file";
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
                if (isMaster) {
                    if (MasterNode.streamFileToServer(fd, true)) {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("Unable to update file").setSuccess(false).build());
                    }
                    ackStreamObserver.onCompleted();
                } else {
                    if (SlaveNode.put(username, filepath)) {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("Unable to update file in DB").setSuccess(false).build());
                    }
                    ackStreamObserver.onCompleted();
                }
            }
        };
        return fileDataStreamObserver;
    }


}