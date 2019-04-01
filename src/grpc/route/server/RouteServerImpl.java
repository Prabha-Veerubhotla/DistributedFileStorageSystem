package grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.lang.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lease.Dhcp_Lease_Test;
import main.db.MongoDBHandler;
import main.db.RedisHandler;
import main.entities.FileEntity;
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
    private static String myPort = "2346";
    private static List<String> slaveips = new ArrayList<>();
    private static Dhcp_Lease_Test dhcp_lease_test = new Dhcp_Lease_Test();
    static MongoDBHandler mh = new MongoDBHandler();
    static RedisHandler rh = new RedisHandler();
    private static boolean done = false;
    private static String slave1 = "localhost";
    private static ManagedChannel ch;
    private static FileServiceGrpc.FileServiceStub ayncStub;
    private static ManagedChannel ch1;


    public void getSlaveIpList() {
        Map<String, List<String>> map = dhcp_lease_test.getCurrentNodeMapping();
        if (map.containsKey("slave")) {
            slaveips = map.get("slave");
        }
        //slave1 = slaveips.get(0); -- local testing
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
       // ch = ManagedChannelBuilder.forAddress(slave1, Integer.parseInt(myPort.trim())).usePlaintext(true).build();
        //ayncStub = FileServiceGrpc.newStub(ch);
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

    private ManagedChannel slaveIpThread() {
        Thread thread = new Thread() {
            public void run() {
                logger.info("Fetching Ip List of Nodes...");
                getSlaveIpList();
                ch1 = MasterNode.createChannel();
            }
        };
        thread.start();
        return ch1;
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
        if(isMaster) {
            ch1 = slaveIpThread();
        }
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
                    logger.info("channel is shutitng down");
                    ch1.shutdown();

                } else {
                    if (SlaveNode.put(username, filepath)) {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("Unable to save file in DB").setSuccess(false).build());
                    }
                    ackStreamObserver.onCompleted();
                    //ch1.shutdown();
                }
            }
        };
        return fileDataStreamObserver;
    }

    @Override
    public void deleteFile(FileInfo fileInfo, StreamObserver<Ack> ackStreamObserver) {
        slaveIpThread();
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
        ch1.shutdown();
    }

    @Override
    public void searchFile(FileInfo fileInfo, StreamObserver<Ack> ackStreamObserver) {
        slaveIpThread();
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
        ch1.shutdown();
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
        slaveIpThread();
        FileResponse.Builder fileResponse = FileResponse.newBuilder();

        if (isMaster) {
            fileResponse.setFilename(MasterNode.listFilesInServer(userInfo));

        } else {
            fileResponse.setFilename(SlaveNode.list(userInfo));

        }

        fileResponseStreamObserver.onNext(fileResponse.build());
        fileResponseStreamObserver.onCompleted();
        ch1.shutdown();
    }


    @Override
    public StreamObserver<FileData> updateFile(StreamObserver<Ack> ackStreamObserver) {
        slaveIpThread();
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
                    ackStatus = SlaveNode.update(fileData);
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
                    ch1.shutdown();
                }
            }
        };
        return fileDataStreamObserver;
    }

    @Override
    public void downloadFile(FileInfo fileInfo, StreamObserver<FileData> fileDataStreamObserver) {


        if (isMaster) {
            logger.info("creating channel -download");
            ch1 = slaveIpThread();
            ayncStub = FileServiceGrpc.newStub(ch1);
            logger.info("getting information of " + fileInfo.getFilename().getFilename() + " from server");
            CountDownLatch cdl = new CountDownLatch(1);
            StreamObserver<FileData> fileDataStreamObserver1 = new StreamObserver<FileData>() {
                @Override
                public void onNext(FileData fileData) {
                    logger.info("received file data with seq num: " + fileData.getSeqnum());
                    fileDataStreamObserver.onNext(fileData);
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.info("Exception in the response from server: " + throwable);
                    cdl.countDown();
                }

                @Override
                public void onCompleted() {
                    logger.info("Slave is done sending data");
                    cdl.countDown();
                    logger.info("calling on completed");
                    fileDataStreamObserver.onCompleted();
                    logger.info("shutting down channel");
                    ch1.shutdown();
                }
            };
            ayncStub.downloadFile(fileInfo, fileDataStreamObserver1);
            //logger.info("content: "+ new String(result.getContent().toByteArray()));
            try {
                cdl.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                logger.info("Exception while waiting for count down latch: " + ie);
            }

        } else {
            FileData.Builder fileData1 = FileData.newBuilder();
            FileEntity fileEntity = SlaveNode.get(fileInfo);
            File fn = new File(fileEntity.getFileName());
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
                    fileData1.setContent(ByteString.copyFrom(raw, 0, n));
                    fileData1.setSeqnum(seq);
                    fileData1.setUsername(fileInfo.getUsername());
                    fileData1.setFilename(fileInfo.getFilename());
                    seq++;
                    fileDataStreamObserver.onNext(fileData1.build());
                    logger.info("sending data with seq num: "+fileData1.getSeqnum());
                }
            } catch (IOException io) {
                io.printStackTrace();
            }
            logger.info("calling on completed");
            fileDataStreamObserver.onCompleted();


        }

    }


}