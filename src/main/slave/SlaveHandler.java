package main.slave;

import main.db.DbHandler;
import main.db.MongoDBHandler;
import main.db.RedisHandler;
import main.entities.FileEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SlaveHandler {
    protected static Logger logger = LoggerFactory.getLogger("slave-handler");
    private DbHandler redis;
    private DbHandler mongoDB;
    public static final boolean CacheUnabled = false;

    /**
     * initialize Redis and Mongo connections
     */
    public SlaveHandler() {
        try {
            if(CacheUnabled) {
                redis = new RedisHandler();
                redis.initDatabaseHandler();
            }
            mongoDB = new MongoDBHandler();
            mongoDB.initDatabaseHandler();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * stores new file partition into MongoDB and Redis
     * @param userEmail
     * @param file
     */
    public void createNewFile(String userEmail, FileEntity file) {
        if(CacheUnabled) {
            String redisRet = redis.put(userEmail, file);
            System.out.println("Redis Output: " + redisRet);
        }
        String mongoRet = mongoDB.put(userEmail, file);
        System.out.println("Mongo Output: " + mongoRet);
    }

    /**
     * Retrieves file partition from Redis or on cache miss from MongoDB
     * @param email
     * @param fileName
     * @return
     */
    public FileEntity retrieveFile(String email, String fileName){
        FileEntity reqFile = null;
        if(CacheUnabled) {
            reqFile = redis.get(email, fileName);
            System.out.println("File here: " + reqFile);
        }
        if(reqFile == null){
            System.out.println("File not found in Redis!");
            reqFile = mongoDB.get(email,fileName);
            if(CacheUnabled) {
                System.out.println("Updating Cache.");
                redis.put(email, reqFile);
            }
        }
        return reqFile;
    }

    /**
     * Deletes file partition from Redis and MongoDB
     * @param email
     * @param fileName
     */
    public boolean removeFile(String email, String fileName){
        if(CacheUnabled) {
            redis.remove(email, fileName);
        }
        mongoDB.remove(email, fileName);
        return true;
    }

    /**
     * Updates Redis and MongoDB with file changes
     * @param email
     * @param newFile
     */
    public void updateFile(String email, FileEntity newFile){
        if(CacheUnabled) {
            redis.update(email, newFile);
        }
        mongoDB.update(email, newFile);
    }

    /**
     * Gets all file partitions for a user
     * @param email
     * @return
     */
    public List<FileEntity> getAllFiles(String email){
        return ((MongoDBHandler)mongoDB).get(email);
    }

    public static void main(String[] args) {
        SlaveHandler h = new SlaveHandler();
//
//        byte[] cont = new byte[100];
        String test1 = "Using Redis!!!!!";
        byte[] test1_byte = test1.getBytes();
//        h.createNewFile("nrupa.test@gmail.edu", new FileEntity("test1.txt", test1_byte));
//        FileEntity reqFile = h.retrieveFile("nrupa.test@sjsu.edu", "test1.txt");
//        System.out.println("File: " + reqFile.getFileContents());
//        List<FileEntity> ans = h.getAllFiles("nrupa.chitley@sjsu.edu");
//        System.out.println("ALL Files: " + ans);
//        h.removeFile("nrupa.chitley@sjsu.edu", "/save/file");
//        List<FileEntity> ans = ((MongoDBHandler)h.mongoDB).get("nrupa.chitley@sjsu.edu");
//        System.out.println("Ans: " + ans);
//        h.updateFile("nrupa.test@sjsu.edu", new FileEntity("test1.txt", "Update Test Content!!!!!!!!!"));
    }
}
