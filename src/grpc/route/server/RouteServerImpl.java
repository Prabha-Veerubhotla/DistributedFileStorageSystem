package grpc.route.server;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;
import java.lang.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.mongodb.connection.Cluster;
import com.sun.management.UnixOperatingSystemMXBean;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lease.Dhcp_Lease_Test;
import main.db.MongoDBHandler;
import main.db.RedisHandler;
import main.entities.FileEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fileservice.*;
import utility.FetchConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import static grpc.route.server.MasterNode.*;


public class RouteServerImpl extends FileserviceGrpc.FileserviceImplBase {
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
    private static String slave1 = "localhost";
    private static FileserviceGrpc.FileserviceStub asyncStub;
    private static FileserviceGrpc.FileserviceBlockingStub blockingStub;
    private static FileserviceGrpc.FileserviceStub replicaStub;
    private static ManagedChannel ch1;
    private static MasterMetaData masterMetaData = new MasterMetaData();
    ManagedChannel originalChannel = null;
    ManagedChannel replicaChannel = null;
    private CountDownLatch cdl = new CountDownLatch(1);
    StreamObserver<FileData> fdsm;
    List<ManagedChannel> managedChannelList = new ArrayList<>();
    String originalIp;
    String replicaIp;
    boolean isChannelCreated = false;
    private int seqID = 1;
    private int repSeqID = 1;
    // for master to send its ip to supernode
    private static String myIP = null;
    private static boolean supernode = false;
    // for storing slave stats
    List<String> cpuUsages = new ArrayList<>();
    List<String> totalDiskSpace = new ArrayList<>();
    List<String> usedDiskSpace = new ArrayList<>();

    Map<ClusterInfo, ClusterStats> clusterStatsMap = new HashMap<>();


    public void getSlaveIpList() {
        Map<String, List<String>> map = dhcp_lease_test.getCurrentNodeMapping();
        if (map.containsKey("slave")) {
            slaveips = map.get("slave");
        }
        //slave1 = slaveips.get(0); -- local testing
        slave1 = MasterNode.assignSlaveIp(slaveips);
    }

    @SuppressWarnings("unchecked")
    public static byte[] combineBytes(Map<String, byte[]> res) {
        List<String> sortedKeys = new ArrayList(res.keySet());
        sortedKeys.sort(Comparator.comparingInt(Integer::parseInt));
        List<byte[]> allbytes = new ArrayList<>();
        for (String sortedKey : sortedKeys) {
            allbytes.add(res.get(sortedKey));
        }
        System.out.println("Total Size: " + allbytes.size());
        List<Byte> allData = new ArrayList<>();
        for (byte[] allbyte : allbytes) {
            for (byte anAllbyte : allbyte) {
                allData.add(anAllbyte);
            }
        }

        byte[] b = new byte[allData.size()];
        for (int i = 0; i < allData.size(); i++) {
            b[i] = allData.get(i);
        }
        System.out.println("Total BSize: " + b.length);
        return b;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            logger.info("Missing server configuration");
            return;
        }
        String path = args[0];
        Properties conf = FetchConfig.getConfiguration(new File(path));

        RouteServer.configure(conf);

        //TODO: Integrate Leader Election here
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

    private void start() throws Exception {
        svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteServerImpl())
                .build();

        logger.info("Starting server..");
        svr.start();

        if (isMaster) {
            invokeDhcpMonitorThread();
            slaveIpThread();
            getSlavesHeartBeat();
        }
        if (supernode) {
            getOtherClusterStats();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                RouteServerImpl.this.stop();
            }
        });
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
                ch1 = MasterNode.createChannel(slave1);

            }
        };
        thread.start();
        return ch1;
    }

    //gets and updates the nodeStatsMap<String ip, Stats stats> in MasterNode every 5 seconds
    private void getSlavesHeartBeat() {
        logger.info("Started monitoring heart beat of slaves..");
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                logger.info("running heart beat monitoring service...");
                if (dhcp_lease_test.getCurrentIpList().size() > 0) {
                    MasterNode.getHeartBeatofAllSlaves();
                }
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(timerTask, 0, 60000);
    }

    private void getOtherClusterStats() {

    }


    protected void stop() {
        svr.shutdown();
    }

    private void blockUntilShutdown() throws Exception {
        svr.awaitTermination();
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
    public void nodeUpdate(UpdateMessage updateMessage, StreamObserver<UpdateMessage> updateMessageStreamObserver) {
        UpdateMessage.Builder um = UpdateMessage.newBuilder();
        um.setMessage("Update received");
        logger.info("Current Node update received from master: " + updateMessage.getMessage());
        updateMessageStreamObserver.onNext(um.build());
        updateMessageStreamObserver.onCompleted();
    }

    public static String getFileName(String filePath) {
        String[] tokens = filePath.split("/");
        String fileName = tokens[tokens.length - 1];
        return fileName;
    }

    public void replicate(FileData fileData, boolean complete, StreamObserver<FileData> fileDataStreamObserver) {
        logger.info("replicating file: " + fileData.getFilename());
        fileservice.FileData.Builder fileData1 = FileData.newBuilder();
        fileData1.setFilename(fileData.getFilename());
        fileData1.setUsername(fileData.getUsername());
        fileData1.setData(fileData.getData());

        if (complete) {
            logger.info("sending completed to slave");
            fileDataStreamObserver.onCompleted();
        } else {
            fileDataStreamObserver.onNext(fileData1.build());
            logger.info("sent data filename:  " + fileData1.getFilename() + " to slave");
        }
        try {
            cdl.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Exception while waiting for count down latch: " + ie);
        }
    }


    @Override
    public StreamObserver<FileData> uploadFile(StreamObserver<ack> ackStreamObserver) {
        logger.info("Calling Upload file");
        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;

            @Override
            public void onNext(FileData fileData) {
                logger.info("Received file:  " + fileData.getFilename() + " for file: " + fileData.getFilename());
                fd = fileData;
                username = fileData.getUsername();
                filepath = fileData.getFilename();

                if (isMaster) {
                    if (!MasterNode.isRoundRobinCalled) {
                        List<String> slaveIpList = dhcp_lease_test.getCurrentIpList();
                        MasterNode.assignSlaveIp(slaveIpList);
                        originalIp = roundRobinIP();
                        if (slaveIpList.size() > 1) {
                            replicaIp = roundRobinIP();
                            replicaChannel = ManagedChannelBuilder.forAddress(replicaIp, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
                            managedChannelList.add(replicaChannel);
                            replicaStub = FileserviceGrpc.newStub(replicaChannel);
                            StreamObserver<ack> ackStreamObserver = new StreamObserver<ack>() {

                                @Override
                                public void onNext(ack ack1) {
                                    logger.info("Received ack status from the server: " + ack1.getSuccess());
                                    logger.info("Received ack  message from the server: " + ack1.getMessage());
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                    logger.info("Exception in the response from server: " + throwable);
                                    cdl.countDown();
                                }

                                @Override
                                public void onCompleted() {
                                    logger.info("Server is done sending data");
                                    cdl.countDown();
                                    // replicaChannel.shutdown();
                                }
                            };

                            fdsm = replicaStub.replicateFile(ackStreamObserver);
                        }
                        originalChannel = ManagedChannelBuilder.forAddress(originalIp, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
                        managedChannelList.add(originalChannel);
                        setIsRoundRobinCalled(true);
                    } else {
                        logger.info("round robin not called");
                    }
                    if (managedChannelList.size() > 1) {
                        replicate(fileData, false, fdsm);
                    }
                    ackStatus = MasterNode.streamFileToServer(fileData, false, originalChannel);
                    if (ackStatus) {
                        ackMessage = "success";
                    } else {
                        ackMessage = "Unable to save file";
                    }
                } else {
                    logger.info("received data from master");
                    ackStatus = SlaveNode.put(fileData, Integer.toString(seqID));
                    seqID++;
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
                    if (MasterNode.streamFileToServer(fd, true, originalChannel)) {
                        ackStreamObserver.onNext(ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(ack.newBuilder().setMessage("Unable to save file").setSuccess(false).build());
                    }
                    if (managedChannelList.size() > 1) {
                        replicate(fd, true, fdsm);

                        asyncStub = FileserviceGrpc.newStub(replicaChannel);
                        DataType.Builder datatype = DataType.newBuilder();
                        datatype.setFilename(fd.getFilename());
                        datatype.setType("put");
                        datatype.setUsername(username);

                        StreamObserver<ack> ackStreamObserver = new StreamObserver<ack>() {

                            @Override
                            public void onNext(ack ack1) {
                                logger.info("Received ack status from the server: " + ack1.getSuccess());
                                logger.info("Received ack  message from the server: " + ack1.getMessage());
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                logger.info("Exception in the response from server: " + throwable);
                                cdl.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                logger.info("Server is done sending data");
                                cdl.countDown();
                                replicaChannel.shutdown();
                            }
                        };

                        asyncStub.completeStreaming(datatype.build(), ackStreamObserver);
                    }

                    ackStreamObserver.onCompleted();
                    asyncStub = FileserviceGrpc.newStub(originalChannel);
                    DataType.Builder datatype = DataType.newBuilder();
                    datatype.setFilename(fd.getFilename());
                    datatype.setType("put");
                    datatype.setUsername(username);

                    StreamObserver<ack> ackStreamObserver = new StreamObserver<ack>() {

                        @Override
                        public void onNext(ack ack1) {
                            logger.info("Received ack status from the server: " + ack1.getSuccess());
                            logger.info("Received ack  message from the server: " + ack1.getMessage());
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.info("Exception in the response from server: " + throwable);
                            cdl.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            logger.info("Server is done sending data");
                            cdl.countDown();

                            // replicaChannel.shutdown();
                        }
                    };


                    asyncStub.completeStreaming(datatype.build(), ackStreamObserver);

                    originalChannel.shutdown();
                    logger.info("putting metadata of file, slave in master");
                    logger.info("username: " + username);
                    logger.info("filepath: " + filepath);
                    logger.info("original ip: " + originalIp);
                    logger.info("file name: " + getFileName(filepath));
                    masterMetaData.putMetaData(username, getFileName(filepath), originalIp);
                    masterMetaData.putMetaDataForIP(username, getFileName(filepath), originalIp);
                    if (managedChannelList.size() > 1) {
                        logger.info("putting metadata of replicated file, slave in master");
                        logger.info("username: " + username);
                        logger.info("filepath: " + filepath);
                        logger.info("replica ip: " + replicaIp);
                        logger.info("file name: " + getFileName(filepath));
                        masterMetaData.putMetaData(username, getFileName(filepath), replicaIp);
                        masterMetaData.putMetaDataForIP(username, getFileName(filepath), replicaIp);
                    }
                    MasterNode.isRoundRobinCalled = false;
                    managedChannelList.clear();
                } else {
                    seqID = 1;
                    ackStreamObserver.onCompleted();
                }
            }
        };
        return fileDataStreamObserver;
    }

    @Override
    public void fileDelete(FileInfo fileInfo, StreamObserver<ack> ackStreamObserver) {
        logger.info("Calling delete file for: " + fileInfo.getFilename());
        ack.Builder ack1 = ack.newBuilder();
        boolean ackStatus;
        String ackMessage = "Unable to save file";

        if (isMaster) {
            List<String> ips = masterMetaData.getMetaData(fileInfo.getUsername(), getFileName(fileInfo.getFilename()));

            logger.info("deleting metadata of file, slave in master");
            logger.info("username: " + fileInfo.getUsername());
            logger.info("filepath: " + fileInfo.getFilename());
            logger.info("file name: " + getFileName(fileInfo.getFilename()));
            masterMetaData.deleteFileFormMetaData(fileInfo.getUsername(), getFileName(fileInfo.getFilename()));

            for (String ip : ips) {
                managedChannelList.add(MasterNode.createChannel(ip));
                ackStatus = MasterNode.deleteFileFromServer(fileInfo);
                if (ackStatus) {
                    ackMessage = "success";
                }
                logger.info("ack status: " + ackStatus);
                logger.info("ack message: " + ackMessage);

                ack1.setMessage(ackMessage);
                ack1.setSuccess(ackStatus);
                ackStreamObserver.onNext(ack1.build());
                ackStreamObserver.onCompleted();
            }

            for (ManagedChannel managedChannel : managedChannelList) {
                managedChannel.shutdown();
            }
        } else {
            ackStatus = SlaveNode.delete(fileInfo);
            if (ackStatus) {
                ackMessage = "success";
            }
            ack1.setMessage(ackMessage);
            ack1.setSuccess(ackStatus);
            ackStreamObserver.onNext(ack1.build());
            ackStreamObserver.onCompleted();
        }
    }

    public static boolean search(FileInfo fileInfo) {
        logger.info("Searching for file: " + fileInfo.getFilename() + " in DB.");
        return new MasterMetaData().checkIfFileExists(fileInfo.getUsername(), getFileName(fileInfo.getFilename()));

    }

    @Override
    public void fileSearch(FileInfo fileInfo, StreamObserver<ack> ackStreamObserver) {
        ack.Builder ack1 = ack.newBuilder();
        boolean ackStatus = false;
        String ackMessage = "File is not present";
        if (isMaster) {
            ackStatus = search(fileInfo);
            if (ackStatus) {
                ackMessage = "present";
            }
        }
        ack1.setMessage(ackMessage);
        ack1.setSuccess(ackStatus);
        ackStreamObserver.onNext(ack1.build());
        ackStreamObserver.onCompleted();
    }

    @Override
    public void fileList(UserInfo userInfo, StreamObserver<FileListResponse> fileResponseStreamObserver) {
        logger.info("Listing files for user: " + userInfo.getUsername());
        FileListResponse.Builder fileResponse = FileListResponse.newBuilder();
        if (isMaster) {
            fileResponse.setFilenames(masterMetaData.getAllFiles(userInfo.getUsername()).toString());
        }
        fileResponseStreamObserver.onNext(fileResponse.build());
        fileResponseStreamObserver.onCompleted();
    }


    @Override
    public StreamObserver<FileData> updateFile(StreamObserver<ack> ackStreamObserver) {
        logger.info("calling update file");

        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;


            @Override
            public void onNext(FileData fileData) {
                logger.info("Received file:  " + fileData.getFilename());
                fd = fileData;
                username = fileData.getUsername();
                filepath = fileData.getFilename();
                if (isMaster) {
                    if (!isChannelCreated) {
                        List<String> ips = masterMetaData.getMetaData(username, getFileName(filepath));
                        logger.info("meta data: " + ips + "  for file: " + filepath);
                        ManagedChannel managedChannel;
                        for (int i = 0; i < ips.size(); i++) {
                            if (i == 0) {
                                managedChannel = createChannel(ips.get(i));
                                originalIp = ips.get(i);
                            } else {
                                replicaIp = ips.get(i);
                                managedChannel = ManagedChannelBuilder.forAddress(replicaIp, Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
                            }
                            managedChannelList.add(managedChannel);
                        }
                        isChannelCreated = true;
                    }
                    logger.info("managed channel list size: " + managedChannelList.size());
                    StreamObserver<ack> ackStreamObserver = new StreamObserver<ack>() {

                        @Override
                        public void onNext(ack ack1) {
                            logger.info("Received ack status from the server: " + ack1.getSuccess());
                            logger.info("Received ack  message from the server: " + ack1.getMessage());
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.info("Exception in the response from server: " + throwable);
                            cdl.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            logger.info("Server is done sending data");
                            cdl.countDown();

                            // replicaChannel.shutdown();
                        }
                    };

                    ackStatus = MasterNode.updateFileToServer(fileData, false);
                    if (managedChannelList.size() > 1 && replicaIp != null) {
                        logger.info("entering replica part");
                        replicaStub = FileserviceGrpc.newStub(managedChannelList.get(1));
                        fdsm = replicaStub.updateReplicateFile(ackStreamObserver);
                        replicate(fileData, false, fdsm);
                    }
                    if (ackStatus) {
                        ackMessage = "success";
                    } else {
                        ackMessage = "Unable to update file";
                    }
                } else {
                    //logger.info("Updating in redis" + new String(fileData.getContent().toByteArray()));
                    ackStatus = SlaveNode.update(fileData, Integer.toString(seqID));
                    seqID++;
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
                    if (MasterNode.updateFileToServer(fd, true)) {
                        ackStreamObserver.onNext(ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(ack.newBuilder().setMessage("Unable to update file").setSuccess(false).build());
                    }
                    ackStreamObserver.onCompleted();

                    asyncStub = FileserviceGrpc.newStub(originalChannel);
                    DataType.Builder datatype = DataType.newBuilder();
                    datatype.setFilename(fd.getFilename());
                    datatype.setType("update");
                    datatype.setUsername(username);

                    StreamObserver<ack> ackStreamObserver = new StreamObserver<ack>() {

                        @Override
                        public void onNext(ack ack1) {
                            logger.info("Received ack status from the server: " + ack1.getSuccess());
                            logger.info("Received ack  message from the server: " + ack1.getMessage());
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.info("Exception in the response from server: " + throwable);
                            cdl.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            logger.info("Server is done sending data");
                            cdl.countDown();

                            // replicaChannel.shutdown();
                        }
                    };


                    asyncStub.completeStreaming(datatype.build(), ackStreamObserver);


                    masterMetaData.deleteFileFormMetaData(username, getFileName(filepath));
                    if (managedChannelList.size() > 1 && replicaIp != null) {
                        replicate(fd, true, fdsm);
                        DataType.Builder datatype1 = DataType.newBuilder();
                        datatype1.setFilename(fd.getFilename());
                        datatype1.setType("update");
                        datatype1.setUsername(username);

                        StreamObserver<ack> ackStreamObserver1 = new StreamObserver<ack>() {

                            @Override
                            public void onNext(ack ack1) {
                                logger.info("Received ack status from the server: " + ack1.getSuccess());
                                logger.info("Received ack  message from the server: " + ack1.getMessage());
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                logger.info("Exception in the response from server: " + throwable);
                                cdl.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                logger.info("Server is done sending data");
                                cdl.countDown();

                                // replicaChannel.shutdown();
                            }
                        };

                        replicaStub.completeStreaming(datatype1.build(), ackStreamObserver1);

                        logger.info("putting metadata of file, slave in master");
                        logger.info("username: " + username);
                        logger.info("filepath: " + filepath);
                        logger.info("replica ip: " + replicaIp);
                        logger.info("file name: " + getFileName(filepath));
                        masterMetaData.putMetaData(username, getFileName(filepath), replicaIp);
                        masterMetaData.putMetaDataForIP(username, getFileName(filepath), replicaIp);
                    }
                    logger.info("putting metadata of  replica file, slave in master");
                    logger.info("username: " + username);
                    logger.info("filepath: " + filepath);
                    logger.info("original ip: " + originalIp);
                    logger.info("file name: " + getFileName(filepath));
                    masterMetaData.putMetaData(username, getFileName(filepath), originalIp);
                    masterMetaData.putMetaDataForIP(username, getFileName(filepath), originalIp);
                    logger.info("channel is shutitng down");
                    for (ManagedChannel channel : managedChannelList) {
                        channel.shutdown();
                    }
                    managedChannelList.clear();
                } else {
                    seqID = 1;
                    ackStreamObserver.onCompleted();
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
            asyncStub = FileserviceGrpc.newStub(ch1);
            String username = fileInfo.getUsername();
            String filename = fileInfo.getFilename();
            logger.info("getting information of " + fileInfo.getFilename() + " from server");
            CountDownLatch cdl = new CountDownLatch(1);
            StreamObserver<FileData> fileDataStreamObserver1 = new StreamObserver<FileData>() {

                @Override
                public void onNext(FileData fileData) {
                    logger.info("received file: " + fileData.getFilename());
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

            List<String> ips = masterMetaData.getMetaData(username, getFileName(filename));
            for (int i = 0; i < ips.size(); i++) {
                if (!new Dhcp_Lease_Test().getCurrentIpList().contains(ips.get(i))) {
                    continue;
                }
                ch1 = MasterNode.createChannel(ips.get(i));
            }
            if (ch1 != null) {
                asyncStub = FileserviceGrpc.newStub(ch1);
                asyncStub.downloadFile(fileInfo, fileDataStreamObserver1);
            }
            try {
                cdl.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                logger.info("Exception while waiting for count down latch: " + ie);
            }

        } else {
            FileData.Builder fileData1 = FileData.newBuilder();
            FileEntity fileEntity = SlaveNode.get(fileInfo);
            Map<String, byte[]> res = (Map<String, byte[]>) fileEntity.getFileContents();
            byte[] temp = combineBytes(res);
            BufferedOutputStream bw = null;
            try {
                bw = new BufferedOutputStream(new FileOutputStream("output-" + fileEntity.getFileName()));
                bw.write(temp);
                bw.flush();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            File fn = new File("output-" + fileEntity.getFileName());
            logger.info("FileEntity Name:" + "output-" + fileEntity.getFileName().toString());
            FileInputStream fis = null;
            try {
                logger.info("FileName:" + fn.toString());
                fis = new FileInputStream(fn);
                logger.info("fis: " + fis.toString());
                logger.info("file length: " + fn.length());
                long seq = 0l;
                final int blen = 4194000;
                byte[] raw = new byte[blen];
                boolean done = false;
                while (!done) {
                    int n = fis.read(raw, 0, blen);
                    logger.info("n: " + n);
                    if (n <= 0)
                        break;
                    // identifying sequence number
                    fileData1.setData(ByteString.copyFrom(raw, 0, n));
                    fileData1.setUsername(fileInfo.getUsername());
                    fileData1.setFilename(fileInfo.getFilename());
                    seq++;
                    fileDataStreamObserver.onNext(fileData1.build());
                    logger.info("sending data of file: " + fileData1.getFilename());
                }
            } catch (IOException io) {
                io.printStackTrace();
            }
            logger.info("calling on completed");
            fileDataStreamObserver.onCompleted();


        }

    }

    @Override
    public StreamObserver<FileData> updateReplicateFile(StreamObserver<ack> ackStreamObserver) {
        logger.info("Calling update replicate file");
        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;

            @Override
            public void onNext(FileData fileData) {
                fd = fileData;
                username = fileData.getUsername();
                filepath = fileData.getFilename();
                logger.info("received data from master");
                ackStatus = SlaveNode.update(fileData, Integer.toString(repSeqID));
                repSeqID++;
                if (ackStatus) {
                    ackMessage = "success";
                } else {
                    ackMessage = "Unable to update file";
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the request from node: " + throwable);
            }

            @Override
            public void onCompleted() {
                logger.info("Node is done sending messages");
                repSeqID = 1;
                ackStreamObserver.onCompleted();
            }
        };
        return fileDataStreamObserver;
    }

    @Override
    public StreamObserver<FileData> replicateFile(StreamObserver<ack> ackStreamObserver) {
        logger.info("Calling replicate file");
        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;

            @Override
            public void onNext(FileData fileData) {
                fd = fileData;
                username = fileData.getUsername();
                filepath = fileData.getFilename();
                logger.info("received data from master");
                ackStatus = SlaveNode.put(fileData, Integer.toString(repSeqID));
                repSeqID++;
                if (ackStatus) {
                    ackMessage = "success";
                } else {
                    ackMessage = "Unable to save file";
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the request from node: " + throwable);
            }

            @Override
            public void onCompleted() {
                logger.info("Node is done sending messages");
                repSeqID = 1;
                ackStreamObserver.onCompleted();
            }
        };
        return fileDataStreamObserver;
    }

    //This is slave's service.
    @Override
    public void isAlive(NodeInfo request, StreamObserver<ClusterStats> responseObserver) {

        sendStatstoMaster(responseObserver);

    }

    public void sendStatstoMaster(StreamObserver<ClusterStats> responseObserver) {

        OperatingSystemMXBean mxBean = ManagementFactory.getPlatformMXBean(UnixOperatingSystemMXBean.class);

        ClusterStats.Builder stats = ClusterStats.newBuilder();
        stats.setCpuUsage(Double.toString(((UnixOperatingSystemMXBean) mxBean).getSystemCpuLoad()));
        File systemFile = new File("/");
        stats.setDiskSpace(Double.toString(systemFile.getTotalSpace()));
        stats.setUsedMem(Double.toString(systemFile.getTotalSpace() - systemFile.getFreeSpace()));
//        stats.setUsedMem(Double.toString(mxBean.))

        responseObserver.onNext(stats.build());
        responseObserver.onCompleted();

    }

    @Override
    public void getClusterStats(Empty empty, StreamObserver<ClusterStats> statsStreamObserver) {

        List<String> availableNodes = new Dhcp_Lease_Test().getCurrentIpList();

        if (isMaster) {
            for (int i = 0; i < availableNodes.size(); i++) {
                StreamObserver<ClusterStats> slaveStatsStreamObserver = new StreamObserver<ClusterStats>() {


                    @Override
                    public void onNext(ClusterStats clusterStats) {
                        cpuUsages.add(clusterStats.getCpuUsage());
                        totalDiskSpace.add(clusterStats.getDiskSpace());
                        usedDiskSpace.add(clusterStats.getUsedMem());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.info("Exception in the request from node: " + throwable);
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("Received stats from slaves");
                    }
                };

                ManagedChannel channel = ManagedChannelBuilder.forAddress(availableNodes.get(i), 2345).usePlaintext().build();
                asyncStub = FileserviceGrpc.newStub(channel);
                Empty.Builder empty1 = Empty.newBuilder();
                asyncStub.getClusterStats(empty1.build(), statsStreamObserver);


                /*cpuUsages.add(clusterStats.getCpuUsage());
                totalDiskSpace.add(clusterStats.getDiskSpace());
                usedDiskSpace.add(clusterStats.getUsedMem());
                channel.shutdown();*/
            }
        } else {
            OperatingSystemMXBean mxBean = ManagementFactory.getPlatformMXBean(UnixOperatingSystemMXBean.class);

            ClusterStats.Builder slaveStats = ClusterStats.newBuilder();
            slaveStats.setCpuUsage(Double.toString(((UnixOperatingSystemMXBean) mxBean).getSystemCpuLoad()));
            File systemFile = new File("/");
            slaveStats.setDiskSpace(Double.toString(systemFile.getTotalSpace()));
            slaveStats.setUsedMem(Double.toString(systemFile.getTotalSpace() - systemFile.getFreeSpace()));


            statsStreamObserver.onNext(slaveStats.build());
            statsStreamObserver.onCompleted();
        }

    }

    // If super node then listen to cluster master
    @Override
    public void getLeaderInfo(fileservice.ClusterInfo request,
                              io.grpc.stub.StreamObserver<fileservice.ack> ackObserver) {

    }

    @Override
    public void completeStreaming(DataType dataType, StreamObserver<ack> ackStreamObserver) {
        logger.info("Calling complete streaming");
        if(dataType.getType().equalsIgnoreCase("put")) {
            if (SlaveNode.put(dataType.getUsername(), dataType.getFilename())) {
                ackStreamObserver.onNext(ack.newBuilder().setMessage("success").setSuccess(true).build());
            } else {
                ackStreamObserver.onNext(ack.newBuilder().setMessage("Unable to update file in DB").setSuccess(false).build());
            }
        }
        if(dataType.getType().equalsIgnoreCase("update")) {
            if (SlaveNode.updateMongo(dataType.getUsername(), dataType.getFilename())) {
                ackStreamObserver.onNext(ack.newBuilder().setMessage("success").setSuccess(true).build());
            } else {
                ackStreamObserver.onNext(ack.newBuilder().setMessage("Unable to update file in DB").setSuccess(false).build());
            }
        }
        ackStreamObserver.onCompleted();
    }
}