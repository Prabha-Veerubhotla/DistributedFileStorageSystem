package grpc.route.server;

import com.google.protobuf.ByteString;
import com.sun.management.UnixOperatingSystemMXBean;
import io.grpc.stub.StreamObserver;
import main.entities.FileEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;


//For HeartBeat
import route.FileResponse;
import route.HeartBeatGrpc;
import route.NodeInfo;
import route.Stats;



public class SlaveNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-slave");


    /**
     *
     */
    public static String getFileName(String filePath) {
        String[] tokens = filePath.split("/");
        String fileName = tokens[tokens.length - 1];
        return fileName;
    }

    /**
     * put in Redis
     *
     * @param fileData
     * @return boolean
     */
    public static boolean put(FileData fileData) {
        UserInfo userName = fileData.getUsername();
        byte[] payload = fileData.getContent().toByteArray();
        String seqID = Long.toString(fileData.getSeqnum());
        String fileName = getFileName(fileData.getFilename().getFilename());
        logger.info("Put details: " + userName + " seq num: " + seqID);
        //logger.info("content: " + new String(payload));
        //TODO: store the file in db from method : writeChunksIntoFile -- done
        rh.put(userName.getUsername(), fileName, seqID, payload);
        return true;
    }

    /**
     * put in MongoDB
     *
     * @param userName
     * @param filePath
     */
    public static boolean put(String userName, String filePath) {
        logger.info("SlaveNode.put userName: " + userName + " filePath: " + filePath);
        String fileName = getFileName(filePath);
        logger.info("Completed Streaming File: " + filePath + " Now writing to MongoDB!");
        Map<String, byte[]> res = rh.get(userName, fileName); // SeqID:content
        mh.put(userName, new FileEntity(fileName, res));
        return true;
    }
    //TODO: Handle cache miss

    /**
     * retrieve a file. If file is not in Redis fetch from MongoDB
     *
     * @param fileInfo
     * @return
     */
    public static FileEntity get(FileInfo fileInfo) {
        logger.info("SlaveNode.GET");
        String userName = fileInfo.getUsername().getUsername();
        String fileName = getFileName(fileInfo.getFilename().getFilename());
        logger.info("retrieving information of: " + fileName);
        Map<String, byte[]> result = rh.get(userName, fileName);
        logger.info("Result ---> " + result);
        if (result != null) {
            logger.info("File found in redis!");
            return new FileEntity(fileName, result);
        }
        return mh.get(userName, fileName);
    }


    /**
     * update file contents
     *
     * @param fileData
     */
    public static boolean update(FileData fileData) {
        String userName = fileData.getUsername().getUsername();
        String fileName = getFileName(fileData.getFilename().getFilename());
        String seqID = Long.toString(fileData.getSeqnum());
        byte[] payload = fileData.getContent().toByteArray();
        rh.update(userName, fileName, seqID, payload);
        return true;
    }

    public static boolean updateMongo(String username, String filePath) {
        logger.info("Inside slave updateMongo");
        String fileName = getFileName(filePath);
        Map<String, byte[]> result = rh.get(username, fileName);
        FileEntity fileEntity;
        fileEntity = new FileEntity(fileName, result);
        return mh.update(username, fileEntity);
    }



    /**
     * delete a file
     *
     * @param fileInfo coming from somewhere
     * @return boolean
     */

    public static boolean delete(FileInfo fileInfo) {
        boolean status;
        String userName = fileInfo.getUsername().getUsername();
        String fileName = getFileName(fileInfo.getFilename().getFilename());
        logger.info("deleting file " + fileName + " from Redis.");
        status = rh.remove(userName, fileName);
        logger.info("deleting file " + fileName + " from Mongo.");
        mh.remove(userName, fileName);
        logger.info("delete status: " + status);
        return status;
    }


    class HeartbeatFinder extends HeartBeatGrpc.HeartBeatImplBase {
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

}




/*
    }
//    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
//        String filePath = "temp.jpg";
//        String userName = "prabha";
////        long seq = 0l;
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
//            byte[] temp = combineBytes(r);
//            bw = null;
//            bw = new BufferedOutputStream(new FileOutputStream("tempMongo.jpg"));
//            bw.write(temp);
//            bw.flush();
//            bw.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
// v2: 4. maintain a in memory, cache

 v2: 4. maintain a in memory, cache

*/
