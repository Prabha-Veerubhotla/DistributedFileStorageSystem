package grpc.route.server;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;
import java.lang.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.ByteString;
import com.sun.management.UnixOperatingSystemMXBean;
import io.grpc.ManagedChannel;
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
    private static String myPort = "2345";
    private static List<String> slaveips = new ArrayList<>();
    private static Dhcp_Lease_Test dhcp_lease_test = new Dhcp_Lease_Test();
    static MongoDBHandler mh = new MongoDBHandler();
    static RedisHandler rh = new RedisHandler();
    private static String slave1 = "localhost";
    private static FileServiceGrpc.FileServiceStub ayncStub;
    private static ManagedChannel ch1;
    private static MasterMetaData masterMetaData = new MasterMetaData();


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
            if(dhcp_lease_test.getCurrentIpList().size() > 0) {
                getSlavesHeartBeat();
            }
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
    private void getSlavesHeartBeat(){
        TimerTask timerTask=new TimerTask(){
            @Override
            public void run() {
                MasterNode.getHeartBeatofAllSlaves();
            }
        };
        Timer timer=new Timer();
        timer.scheduleAtFixedRate(timerTask,0,5000);
    }


    protected void stop() {
        svr.shutdown();
    }

    private void blockUntilShutdown() throws Exception {
        svr.awaitTermination();
    }

    public static String getFileName(String filePath) {
        String[] tokens = filePath.split("/");
        String fileName = tokens[tokens.length - 1];
        return fileName;
    }

    @Override
    public StreamObserver<FileData> uploadFile(StreamObserver<Ack> ackStreamObserver) {
        if(isMaster) {
            ch1 = slaveIpThread();
        }
        logger.info("calling upload file");
        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;

            @Override
            public void onNext(FileData fileData) {
                logger.info("Received seq num:  " + fileData.getSeqnum() + " for file: " + fileData.getFilename().getFilename());
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

                    logger.info("putting metadata of file, slave in master");
                    logger.info("username: " + username);
                    logger.info("filepath: " + filepath);
                    logger.info("ip: " + slave1);
                    logger.info("file name: " + getFileName(filepath));
                    masterMetaData.putMetaData(username, getFileName(filepath), slave1);
                    logger.info("channel is shutitng down");
                    ch1.shutdown();

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
        logger.info("Calling delete file for: " + fileInfo.getFilename().getFilename());
        Ack.Builder ack = Ack.newBuilder();
        boolean ackStatus;
        String ackMessage = "Unable to save file";
        if (isMaster) {
            List<String> ips = masterMetaData.getMetaData(fileInfo.getUsername().getUsername(), getFileName(fileInfo.getFilename().getFilename()));
            ch1 = MasterNode.createChannel(ips.get(0));
            ackStatus = MasterNode.deleteFileFromServer(fileInfo);
            if (ackStatus) {
                ackMessage = "success";
            }
            logger.info("ack status: " + ackStatus);
            logger.info("Ack message: " + ackMessage);

            ack.setMessage(ackMessage);
            ack.setSuccess(ackStatus);
            ackStreamObserver.onNext(ack.build());
            ackStreamObserver.onCompleted();

            logger.info("putting metadata of file, slave in master");
            logger.info("username: " + fileInfo.getUsername().getUsername());
            logger.info("filepath: " + fileInfo.getFilename().getFilename());
            logger.info("file name: " + getFileName(fileInfo.getFilename().getFilename()));
            masterMetaData.deleteFileFormMetaData(fileInfo.getUsername().getUsername(), getFileName(fileInfo.getFilename().getFilename()));
            ch1.shutdown();

        } else {
            ackStatus = SlaveNode.delete(fileInfo);
            if (ackStatus) {
                ackMessage = "success";
            }
            ack.setMessage(ackMessage);
            ack.setSuccess(ackStatus);
            ackStreamObserver.onNext(ack.build());
            ackStreamObserver.onCompleted();
        }
    }

    public static boolean search(FileInfo fileInfo) {
        logger.info("Searching for file: " + fileInfo.getFilename().getFilename() + " in DB.");
        return new MasterMetaData().checkIfFileExists(fileInfo.getUsername().getUsername(), getFileName(fileInfo.getFilename().getFilename()));

    }

    @Override
    public void searchFile(FileInfo fileInfo, StreamObserver<Ack> ackStreamObserver) {
        Ack.Builder ack = Ack.newBuilder();
        boolean ackStatus = false;
        String ackMessage = "File is not present";
        if (isMaster) {
            ackStatus = search(fileInfo);
            if (ackStatus) {
                ackMessage = "present";
            }
        }
        ack.setMessage(ackMessage);
        ack.setSuccess(ackStatus);
        ackStreamObserver.onNext(ack.build());
        ackStreamObserver.onCompleted();
    }

    @Override
    public void requestNodeIp(NodeName nodeName, StreamObserver<NodeInfo> nodeInfoStreamObserver) {
        logger.info("Requesting node ip for node: " + nodeName);
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
        logger.info("Listing files for user: " + userInfo.getUsername());
        FileResponse.Builder fileResponse = FileResponse.newBuilder();
        if (isMaster) {
            fileResponse.setFilename(masterMetaData.getAllFiles(userInfo.getUsername()).toString());
        }
        fileResponseStreamObserver.onNext(fileResponse.build());
        fileResponseStreamObserver.onCompleted();
    }


    @Override
    public StreamObserver<FileData> updateFile(StreamObserver<Ack> ackStreamObserver) {
        logger.info("calling update file");
        StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
            boolean ackStatus;
            String ackMessage;
            String username;
            String filepath;
            FileData fd;

            @Override
            public void onNext(FileData fileData) {
                logger.info("Received seq num:  " + fileData.getSeqnum() + " for file: " + fileData.getFilename().getFilename());
                fd = fileData;
                username = fileData.getUsername().getUsername();
                filepath = fileData.getFilename().getFilename();
                if (isMaster) {
                    List<String> ips = masterMetaData.getMetaData(username, getFileName(filepath));
                    ch1 = MasterNode.createChannel(ips.get(0));
                    ackStatus = MasterNode.updateFileToServer(fileData, false);
                    if (ackStatus) {
                        ackMessage = "success";
                    } else {
                        ackMessage = "Unable to update file";
                    }
                } else {
                    logger.info("Updating in redis" + new String(fileData.getContent().toByteArray()));
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
                    if (MasterNode.updateFileToServer(fd, true)) {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("success").setSuccess(true).build());
                    } else {
                        ackStreamObserver.onNext(Ack.newBuilder().setMessage("Unable to update file").setSuccess(false).build());
                    }
                    ackStreamObserver.onCompleted();

                    masterMetaData.deleteFileFormMetaData(username, getFileName(filepath));
                    logger.info("putting metadata of file, slave in master");
                    logger.info("username: " + username);
                    logger.info("filepath: " + filepath);
                    logger.info("ip: " + slave1);
                    logger.info("file name: " + getFileName(filepath));
                    masterMetaData.putMetaData(username, getFileName(filepath), slave1);
                    logger.info("channel is shutitng down");
                    ch1.shutdown();
                } else {
                    logger.info("Calling Update Mongo");
                    if (SlaveNode.updateMongo(username, filepath)) {
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

    @Override
    public void downloadFile(FileInfo fileInfo, StreamObserver<FileData> fileDataStreamObserver) {
        if (isMaster) {
            logger.info("creating channel -download");
            ch1 = slaveIpThread();
            ayncStub = FileServiceGrpc.newStub(ch1);
            String username = fileInfo.getUsername().getUsername();
            String filename = fileInfo.getFilename().getFilename();
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

            List<String> ips = masterMetaData.getMetaData(username, getFileName(filename));
            if (ips.size() > 0) {
                ch1 = MasterNode.createChannel(ips.get(0));
            } else {
                ch1 = MasterNode.createChannel("localhost");
            }
            ayncStub = FileServiceGrpc.newStub(ch1);
            ayncStub.downloadFile(fileInfo, fileDataStreamObserver1);
            try {
                cdl.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                logger.info("Exception while waiting for count down latch: " + ie);
            }

        } else {
            FileData.Builder fileData1 = FileData.newBuilder();
            FileEntity fileEntity = SlaveNode.get(fileInfo);
            Map<String, byte[]> res = (Map<String, byte[]>)fileEntity.getFileContents();
            byte[] temp = combineBytes(res);
            BufferedOutputStream bw = null;
            try {
                bw = new BufferedOutputStream(new FileOutputStream(fileEntity.getFileName()));
                bw.write(temp);
                bw.flush();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            File fn = new File(fileEntity.getFileName());
            logger.info("FileEntity Name:" + fileEntity.toString());
            FileInputStream fis = null;
            try {
              logger.info("FileName:" + fn.toString());
                fis = new FileInputStream(fn);
                logger.info("fis: " + fis.toString());
                long seq = 0l;
                final int blen = 10024;
                byte[] raw = new byte[blen];
                boolean done = false;
                while (!done) {
                    int n = fis.read(raw, 0, blen);
                    logger.info("n: " + n);
                    logger.info("raw before break: " + new String(raw));
                    if (n <= 0)
                        break;
                    logger.info("raw after break: " + new String(raw));
                    System.out.println("n: " + n);
                    // identifying sequence number
                    fileData1.setContent(ByteString.copyFrom(raw, 0, n));
                    fileData1.setSeqnum(seq);
                    fileData1.setUsername(fileInfo.getUsername());
                    fileData1.setFilename(fileInfo.getFilename());
                    seq++;
                    fileDataStreamObserver.onNext(fileData1.build());
                    logger.info("sending data with seq num: " + fileData1.getSeqnum());
                }
            } catch (IOException io) {
                io.printStackTrace();
            }
            logger.info("calling on completed");
            fileDataStreamObserver.onCompleted();


        }

    }

    @Override
    public StreamObserver<FileData> replicateFile(StreamObserver<Ack> ackStreamObserver) {
        //TODO: call from upload file or some where in this file
        if (isMaster) {
            ch1 = slaveIpThread();

        }
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

                    logger.info("putting metadata of file, slave in master");
                    logger.info("username: " + username);
                    logger.info("filepath: " + filepath);
                    logger.info("ip: " + slave1);
                    logger.info("file name: " + getFileName(filepath));
                    masterMetaData.putMetaData(username, getFileName(filepath), slave1);
                    logger.info("channel is shutitng down");
                    ch1.shutdown();

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

    //This is slave's service.
    @Override
    public void isAlive(NodeInfo request, StreamObserver<Stats> responseObserver) {

        sendStatstoMaster(responseObserver);

    }

    public void sendStatstoMaster(StreamObserver<Stats> responseObserver){

        OperatingSystemMXBean mxBean = ManagementFactory.getPlatformMXBean(UnixOperatingSystemMXBean.class);

        Stats.Builder stats = Stats.newBuilder();
        stats.setCpuUsage(Double.toString(((UnixOperatingSystemMXBean) mxBean).getSystemCpuLoad()));
        stats.setDiskSpace(Double.toString(((UnixOperatingSystemMXBean) mxBean).getFreePhysicalMemorySize()));

        responseObserver.onNext(stats.build());
        responseObserver.onCompleted();

    }


}