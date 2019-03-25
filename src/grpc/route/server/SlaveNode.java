package grpc.route.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import main.entities.FileEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import route.RouteServiceGrpc;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class SlaveNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-slave");
    static Map<String, List<String>> map = new HashMap<>();
    private static ManagedChannel ch;
    private static RouteServiceGrpc.RouteServiceStub stub;
    private static String slave1port = "2345";

    /**
     *
     */
    public static String getFileName(String filePath){
        String[] tokens = filePath.split("/");
        String fileName = tokens[tokens.length - 1];
        return fileName;
    }

    /**
     * put in Redis
     * @param r route
     * @return
     */
    public static boolean put(Route r) {
        String userName = r.getUsername();
        byte[] payload = r.getPayload().toByteArray();
        String seqID = Long.toString(r.getSeq());
        String fileName = getFileName(r.getPath());
        logger.info("Put details: "+ userName +" " + " "+ fileName + " "+ seqID);
        //TODO: store the file in db from method : writeChunksIntoFile -- done
        rh.put(userName, fileName, seqID, payload);
        return true;
    }

    /**
     * put in MongoDB
     * @param userName
     * @param filePath
     */
    public static void put(String userName, String filePath) {
        logger.info("SlaveNode.put userName " + userName + " filePath: " + filePath);
        String fileName = getFileName(filePath);
        logger.info("Completed Streaming File: "+filePath+" Now writing to MongoDB!");
        Map<String, byte[]> res = rh.get(userName, fileName); // SeqID:content
        mh.put(userName, new FileEntity(fileName, res));
    }

    //TODO: Handle cache miss
    /**
     * retrieve a file. If file is not in Redis fetch from MongoDB
     * @param r
     * @return
     */
    public static FileEntity get(Route r) {
        logger.info("SlaveNode.GET");
        String userName = r.getUsername();
        String fileName = getFileName(r.getPath());
        logger.info("retrieving information of: " + fileName);
        String name = r.getUsername();
        Map<String, byte[]> result = rh.get(name, fileName);
        if(result != null) {
            return new FileEntity(fileName, result);
        }
       return mh.get(userName, fileName);
    }

    /**
     * delete a file
     * @param r route coming from somewhere
     * @return
     */
    public static boolean delete(Route r) {
        boolean status = false;
        String userName = r.getUsername();
        String fileName = getFileName(r.getPath());
        logger.info("deleting file " + fileName + "from Redis.");
        status = rh.remove(userName, fileName);
        logger.info("deleting file " + fileName + "from Mongo.");
        mh.remove(userName, fileName);
        return status;
    }

    //TODO: Move to client - wrote here for testing purposes
    @SuppressWarnings("unchecked")
    public static byte[] combineBytes(Map<String, byte[]> res){
        List<String> sortedKeys = new ArrayList(res.keySet());
        sortedKeys.sort(Comparator.comparingInt(Integer::parseInt));
        List<byte[]> allbytes = new ArrayList<>();
        for (String sortedKey : sortedKeys) {
            allbytes.add(res.get(sortedKey));
        }
        logger.info("Total Size: " + allbytes.size());
        List<Byte> allData = new ArrayList<>();
        for (byte[] allbyte : allbytes) {
            for (byte anAllbyte : allbyte) {
                allData.add(anAllbyte);
            }
        }

        byte[] b = new byte[allData.size()];
        for (int i = 0; i< allData.size();i++) {
            b[i] = allData.get(i);
        }
        logger.info("Total BSize: " + b.length);
        return b;
    }

    //return file in chunks to the master
    public static void returnFileInchunks(Route r) {
        logger.info("returnFileInchunks");
        ch = ManagedChannelBuilder.forAddress(r.getOrigin(), Integer.parseInt("2345")).usePlaintext(true).build();
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
        logger.info("Streaming file: "+new String(r.getPayload().toByteArray()));
        FileEntity fileEntity = get(r);
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
            ; // ignore? really? ...yes
             requestObserver.onError(e);
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        route.Route.Builder builder = Route.newBuilder();
        builder.setPath(r.getPath());
        builder.setPayload(ByteString.copyFrom("complete".getBytes()));
        builder.setOrigin(r.getDestination());
        builder.setDestination(r.getOrigin());
        builder.setSeq(0);
        builder.setType(r.getType());
        logger.info("Sending complete message to master");
        requestObserver.onNext(builder.build());

        requestObserver.onCompleted();
    }


//    @SuppressWarnings("unchecked")
//    public static void main(String[] args) {
//        String filePath = "temp.jpg";
//        String userName = "N";
//        long seq = 0l;
//        try{
//            FileInputStream fis = new FileInputStream(filePath);
//            int i = 0;
//            do {
//                byte[] buf = new byte[1024];
//                i = fis.read(buf);
//                if (i != -1 ) {
//                    rh.put(userName, filePath, Long.toString(seq), buf);
//                }
//                seq++;
//            } while (i != -1);
//            byte[] payload = null;
//            Map<String, byte[]> res = rh.get(userName, filePath);
//
//            byte[] temp = combineBytes(res);
//            BufferedOutputStream bw = null;
//            bw = new BufferedOutputStream(new FileOutputStream("tempRedis.jpg"));
//            bw.write(temp);
//            bw.flush();
//            bw.close();
//            logger.info("Putting into DB");
//            mh.put(userName, new FileEntity(filePath, res));
//            FileEntity mongoDBres = mh.get(userName, filePath);
//            Map<String, byte[]> r = (Map<String, byte[]>)mongoDBres.getFileContents();
//            temp = combineBytes(res);
//            bw = null;
//            bw = new BufferedOutputStream(new FileOutputStream("tempMongo.jpg"));
//            bw.write(temp);
//            bw.flush();
//            bw.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
// v2: 4. maintain a in memory, cache

