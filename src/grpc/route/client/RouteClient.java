package grpc.route.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import route.*;


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
    private static FileServiceGrpc.FileServiceStub asyncStub;
    private static FileServiceGrpc.FileServiceBlockingStub blockingStub;
    private Properties setup;
    private String name;
    private static String myIp = "client"; // intially , later master node will assign an ip
    protected static Logger logger = LoggerFactory.getLogger("client");
    boolean ackStatus = false;

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
        asyncStub = FileServiceGrpc.newStub(ch);
        blockingStub = FileServiceGrpc.newBlockingStub(ch);
        logger.info("Client running...");
        logger.info("Requesting ip from dhcp server");
        requestIp();
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

    public boolean streamFileToServer(String filename) {
        CountDownLatch cdl = new CountDownLatch(1);
        StreamObserver<Ack> ackStreamObserver = new StreamObserver<Ack>() {

            @Override
            public void onNext(Ack ack) {
                ackStatus = ack.getSuccess();
                logger.info("Received ack status from the server: " + ack.getSuccess());
                logger.info("Received ack  message from the server: " + ack.getMessage());
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
            }
        };

        route.FileData.Builder fileData = FileData.newBuilder();
        route.FileResponse.Builder fileResponse = FileResponse.newBuilder().setFilename(filename);
        fileData.setFilename(fileResponse.build());
        route.UserInfo.Builder userInfo = UserInfo.newBuilder().setUsername(name);
        fileData.setUsername(userInfo.build());

        StreamObserver<FileData> fileDataStreamObserver = asyncStub.uploadFile(ackStreamObserver);
        if (checkIfFile(filename)) {
            logger.info(filename + " is a file");
            File fn = new File(filename);
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
                    fileData.setContent(ByteString.copyFrom(raw, 0, n));
                    logger.info("seq num is: " + seq);
                    fileData.setSeqnum(seq);
                    logger.info("Sending file data to server with seq num: " + seq);
                    fileDataStreamObserver.onNext(fileData.build());
                }
            } catch (IOException e) {
                ; // ignore? really?
                fileDataStreamObserver.onError(e);
            } finally {
                try {
                    fis.close();
                } catch (IOException e) {
                    ; // ignore
                }
            }
        }
        fileDataStreamObserver.onCompleted();

        try {
            cdl.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Exception while waiting for count down latch: " + ie);
        }

        return ackStatus;
    }


    public boolean deleteFileFromServer(String msg) {
        route.FileInfo.Builder fileInfo = FileInfo.newBuilder();
        route.FileResponse.Builder fileResponse = FileResponse.newBuilder().setFilename(msg);
        fileInfo.setFilename(fileResponse.build());
        route.UserInfo.Builder userInfo = UserInfo.newBuilder().setUsername(name);
        fileInfo.setUsername(userInfo.build());

        Ack ack = blockingStub.deleteFile(fileInfo.build());
        return ack.getSuccess();
    }

    public boolean searchFileInServer(String msg) {
        route.FileInfo.Builder fileInfo = FileInfo.newBuilder();
        route.FileResponse.Builder fileResponse = FileResponse.newBuilder().setFilename(msg);
        fileInfo.setFilename(fileResponse.build());
        route.UserInfo.Builder userInfo = UserInfo.newBuilder().setUsername(name);
        fileInfo.setUsername(userInfo.build());

        Ack ack = blockingStub.searchFile(fileInfo.build());
        return ack.getSuccess();
    }


    public void stopClientSession() {
        ch.shutdown();
    }

    //blocking
    public void requestIp() {
        NodeName.Builder nodeName = NodeName.newBuilder();
        nodeName.setName("client");
        NodeInfo nodeInfo = blockingStub.requestNodeIp(nodeName.build());
        myIp = new String(nodeInfo.getIp());
        logger.info("my ip is: " + myIp);
    }
}