package main.slave;

import main.db.DbHandler;
import main.db.MongoDBHandler;
import main.db.RedisHandler;
import main.entities.FileEntity;

import java.util.List;

public class SlaveHandler {
    private DbHandler redis;
    private DbHandler mongoDB;
    public static final boolean CacheUnabled = false;

    SlaveHandler() {
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

    public void createNewFile(String userEmail, FileEntity file) {
        if(CacheUnabled) {
            String redisRet = redis.put(userEmail, file);
            System.out.println("Redis Output.rtf: " + redisRet);
        }
        String mongoRet = mongoDB.put(userEmail, file);
        System.out.println("Mongo Output.rtf: " + mongoRet);
    }

    public FileEntity retrieveFile(String email, String fileName){
        FileEntity reqFile = null;
        if(CacheUnabled) {
            reqFile = redis.get(email, fileName);
            System.out.println("File here: " + reqFile);
        }
        if(reqFile == null){
            System.out.println("File not found in Redis!");
            reqFile = mongoDB.get(email,fileName);
            System.out.println("Updating Cache.");
            redis.put(email, reqFile);
        }
        return reqFile;
    }

    public void removeFile(String email, String fileName){
        if(CacheUnabled) {
            redis.remove(email, fileName);
        }
        mongoDB.remove(email, fileName);
    }

    public void updateFile(String email, FileEntity newFile){
        if(CacheUnabled) {
            redis.update(email, newFile);
        }
        mongoDB.update(email, newFile);
    }

    public List<FileEntity> getAllFiles(String email){
        return ((MongoDBHandler)mongoDB).get(email);
    }

//    public static void main(String[] args) {
//        SlaveHandler h = new SlaveHandler();
//
//        byte[] cont = new byte[100];
//        String test1 = "Using Redis!!!!!";
////        h.createNewFile("nrupa.chitley@sjsu.edu", new FileEntity("test1.txt", test1));
////        FileEntity reqFile = h.retrieveFile("nrupa.chitley@sjsu.edu", "test1.txt");
////        System.out.println("File: " + reqFile.getFileContents());
////        List<FileEntity> ans = h.getAllFiles("nrupa.chitley@sjsu.edu");
////        System.out.println("ALL Files: " + ans);
////        h.removeFile("nrupa.chitley@sjsu.edu", "test1.txt");
////        List<FileEntity> ans = ((MongoDBHandler)h.mongoDB).get("nrupa.chitley@sjsu.edu");
////        System.out.println("Ans: " + ans);
////        h.updateFile("nrupa.chitley@sjsu.edu", new FileEntity("test1.txt", "Update Content!!!!!!!!!"));
//    }
}
