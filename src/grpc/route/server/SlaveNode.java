package grpc.route.server;

import com.google.protobuf.ByteString;
import com.sun.management.UnixOperatingSystemMXBean;
import io.grpc.stub.StreamObserver;
import main.entities.FileEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fileservice.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;


//For HeartBeat
import fileservice.FileListResponse;

import fileservice.NodeInfo;
import fileservice.ClusterStats;



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
    public static boolean put(FileData fileData, String seqID) {
        String userName = fileData.getUsername();
        byte[] payload = fileData.getData().toByteArray();
        String fileName = getFileName(fileData.getFilename());
        logger.info("Put details: " + userName + " seq num: " + seqID);
        rh.put(userName, fileName, seqID, payload);

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

    /**
     * retrieve a file. If file is not in Redis fetch from MongoDB
     *
     * @param fileInfo
     * @return
     */
    public static FileEntity get(FileInfo fileInfo) {
        logger.info("SlaveNode.GET");
        String userName = fileInfo.getUsername();
        String fileName = getFileName(fileInfo.getFilename());
        logger.info("retrieving information of: " + fileName);
        Map<String, byte[]> result = rh.get(userName, fileName);
//        logger.info("Result ---> " + result);
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
    public static boolean update(FileData fileData, String seqID) {
        String userName = fileData.getUsername();
        String fileName = getFileName(fileData.getFilename());
        byte[] payload = fileData.getData().toByteArray();
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
        String userName = fileInfo.getUsername();
        String fileName = getFileName(fileInfo.getFilename());
        logger.info("deleting file " + fileName + " from Redis.");
        status = rh.remove(userName, fileName);
        logger.info("deleting file " + fileName + " from Mongo.");
        mh.remove(userName, fileName);
        logger.info("delete status: " + status);
        return status;
    }

}
