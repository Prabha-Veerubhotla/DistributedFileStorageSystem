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
import fileservice.*;



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


public class RouteClient {
    private static ManagedChannel ch;
    private static FileserviceGrpc.FileserviceStub asyncStub;
    private static FileserviceGrpc.FileserviceBlockingStub blockingStub;
    private Properties setup;
    private String name;
    private static String myIp = "client"; // intially , later master node will assign an ip
    protected static Logger logger = LoggerFactory.getLogger("client");
    boolean ackStatus = false;
    boolean putCompleted = false;
    boolean updateCompleted = false;
    boolean getCompleted = false;

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
        asyncStub = FileserviceGrpc.newStub(ch);
        blockingStub = FileserviceGrpc.newBlockingStub(ch);
        logger.info("Client running...");
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

    public String streamFileToServer(String filename) {
        if (searchFileInServer(filename)) {
            return "File already present";
        } else {
            logger.info("file is not present. saving now");
            CountDownLatch cdl = new CountDownLatch(1);
            StreamObserver<ack> ackStreamObserver = new StreamObserver<ack>() {

                @Override
                public void onNext(ack ack1) {
                    ackStatus = ack1.getSuccess();
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
                    putCompleted = true;
                    cdl.countDown();
                }
            };

            fileservice.FileData.Builder fileData = FileData.newBuilder();

            fileData.setFilename(filename);

            fileData.setUsername(name);

            StreamObserver<FileData> fileDataStreamObserver = asyncStub.uploadFile(ackStreamObserver);

            if (checkIfFile(filename)) {
                logger.info(filename + " is a file");
                File fn = new File(filename);
                logger.info("file length is: "+fn.length());
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(fn);
                    long seq = 0l;
                    final int blen = 4194000;
                    byte[] raw = new byte[blen];
                    boolean done = false;
                    while (!done) {
                        int n = fis.read(raw, 0, blen);
                        logger.info("n: "+n);
                        if (n <= 0)
                            break;
                        // identifying sequence number
                        seq++;
                        logger.info("Streaming seq num: " + seq);
                        fileData.setData(ByteString.copyFrom(raw, 0, n));
                        logger.info("seq num is: " + seq);
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
        }

        while(!putCompleted) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }

        if (ackStatus) {
            return "success";
        }
        return "Unable to save file";
    }


    public String deleteFileFromServer(String msg) {
        if(searchFileInServer(msg)) {
            logger.info("file is present. deleting now");
            fileservice.FileInfo.Builder fileInfo = FileInfo.newBuilder();
            fileInfo.setFilename(msg);
            fileInfo.setUsername(name);

            ack ack1 = blockingStub.fileDelete(fileInfo.build());
            if(ack1.getSuccess()) {
                return "deleted successfully";
            } else {
                return "unable to delete file";
            }
        } return "File not present";
    }

    public boolean searchFileInServer(String msg) {
        logger.info("searching file in server");
        fileservice.FileInfo.Builder fileInfo = FileInfo.newBuilder();
        fileInfo.setFilename(msg);
        fileInfo.setUsername(name);

        ack ack1 = blockingStub.fileSearch(fileInfo.build());
        return ack1.getSuccess();
    }


    public void stopClientSession() {
        ch.shutdown();
    }


    public String listFilesInServer(String userName) {
        UserInfo.Builder userInfo = UserInfo.newBuilder();
        userInfo.setUsername(userName);
        FileListResponse fileResponse = blockingStub.fileList(userInfo.build());
        return fileResponse.getFilenames();
    }

    public String updateFileInServer(String filename) {
        if (searchFileInServer(filename)) {
            logger.info("file is present. updating now");
            CountDownLatch cdl = new CountDownLatch(1);
            StreamObserver<ack> ackStreamObserver = new StreamObserver<ack>() {

                @Override
                public void onNext(ack ack1) {
                    ackStatus = ack1.getSuccess();
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
                    updateCompleted = true;
                }
            };

            fileservice.FileData.Builder fileData = FileData.newBuilder();
            fileData.setFilename(filename);
            fileData.setUsername(name);

            StreamObserver<FileData> fileDataStreamObserver = asyncStub.updateFile(ackStreamObserver);
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
                        fileData.setData(ByteString.copyFrom(raw, 0, n));

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

            while(!updateCompleted) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }

            if (ackStatus) {
                return "success";
            }
        }
        return "Unable to update file";
    }

    public File getFileFromServer(String filename) {
        File file = new File("output-"+filename);
        //Create the file
        try {
            if (file.createNewFile()) {
                logger.info("File: " + file + " is created!");
            } else {
                logger.info("File: " + file + " already exists.");
            }
        if(!checkIfFile(filename)) {
            logger.info("file not present. returning empty file");
            return file;
        }
            RandomAccessFile f = new RandomAccessFile(file, "rw");

            CountDownLatch cdl = new CountDownLatch(1);
            StreamObserver<FileData> fileDataStreamObserver = new StreamObserver<FileData>() {
                @Override
                public void onNext(FileData fileData) {
                    // write into the file , every chunk received from master
                    try {
                        logger.info("writing "+fileData.getFilename());
                        f.write(fileData.getData().toByteArray());
                    } catch (IOException ie) {
                        ie.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.info("Exception in the response from server: " + throwable);
                    cdl.countDown();
                }

                @Override
                public void onCompleted() {
                    try {
                        f.close();
                    } catch (IOException ie) {
                        ie.printStackTrace();
                    }
                    cdl.countDown();
                    getCompleted = true;
                }
            };
            FileInfo.Builder fileInfo = FileInfo.newBuilder();
            fileInfo.setFilename(filename);
            fileInfo.setUsername(name);
            asyncStub.downloadFile(fileInfo.build(), fileDataStreamObserver);
        } catch (IOException io) {
            io.printStackTrace();
        }

        while(!getCompleted) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }

        return file;
    }
}
