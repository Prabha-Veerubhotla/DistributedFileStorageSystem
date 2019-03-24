package grpc.route.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import main.entities.FileEntity;
import main.slave.SlaveHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import route.RouteServiceGrpc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class SlaveNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-slave");
    static Map<String, List<String>> map = new HashMap<>();
    static SlaveHandler sh = new SlaveHandler();
    private static ManagedChannel ch;
    private static RouteServiceGrpc.RouteServiceStub stub;
    private static String slave1port = "2345";

    public static boolean saveFile(String filename, String name, ByteString content) {
        sh.createNewFile(name, new FileEntity(filename, content));
        return true;
    }

    public static boolean put(Route r) {
        String name = r.getUsername();
        String path = r.getPath();
        String payload = r.getPayload().toString();
        logger.info("saving file with seq num: " + r.getSeq());
        sh.createNewFile(name, new FileEntity(path, payload));
        return true;
    }

    public static FileEntity get(Route r) {
        String payload = r.getPayload().toString();
        logger.info("retrieving information of: " + payload);
        String name = r.getUsername();
        FileEntity result = sh.retrieveFile(name, r.getPath());
        return result;
    }

    public static void writeChunksIntoFile(Route r) {
        File file = new File(r.getPath());
        //Create the file
        try {
            if (file.createNewFile()) {
                System.out.println("File is created!");
            } else {
                System.out.println("File already exists.");
            }
            RandomAccessFile f = new RandomAccessFile(file, "rw");
            logger.info("writing chunk with seq num into file: " + r.getSeq());
            f.write(r.getPayload().toByteArray());
            f.close();
        } catch (IOException io) {
            io.printStackTrace();
        }
    }

    //TODO: complete this method
    public static void writeFileInDb() {

    }

    public static void returnFileInchunks(Route r) {
        ch = ManagedChannelBuilder.forAddress(r.getOrigin(), Integer.parseInt(slave1port.trim())).usePlaintext(true).build();
        stub = RouteServiceGrpc.newStub(ch);
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<Route> requestObserver = stub.request(new StreamObserver<Route>() {
            //handle response from server here
            @Override
            public void onNext(Route route) {
                logger.info("Received response from master: " + route.getPayload());
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the response from master: " + throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Server is done sending data");
                latch.countDown();
            }
        });
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Exception while waiting for count down latch: " + ie);
        }
        File fn = new File(r.getPayload().toString());
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

                route.Route.Builder builder = Route.newBuilder();
                builder.setPath(r.getPath());
                builder.setPayload(ByteString.copyFrom(raw, 0, n));
                builder.setOrigin(r.getDestination());
                builder.setDestination(r.getOrigin());
                builder.setSeq(seq);
                builder.setType(r.getType());
                logger.info("Sending file data to master with seq num: " + seq);
                requestObserver.onNext(builder.build());
            }
        } catch (IOException e) {
            ; // ignore? really?
             requestObserver.onError(e);
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        requestObserver.onCompleted();
    }


    public static boolean delete(Route r) {
        boolean status = false;
        String name = r.getUsername();
        String msg = r.getPayload().toString();
        logger.info("deleting message " + msg + " from:  " + name + " in slave..");
        status = sh.removeFile(name, msg);
        return status;
    }

    public static List<FileEntity> list(Route r) {
        logger.info("listing messages or files of: " + r.getUsername());
        String username = r.getUsername();
        List<FileEntity> list = sh.getAllFiles(username);
        logger.info("list of files are: " + list);
        return list;
    }
}
// v2: 4. maintain a in memory, cache

