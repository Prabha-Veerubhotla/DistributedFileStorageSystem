package main.slave;

import main.db.DbHandler;
import main.db.MongoDBHandler;
import main.db.RedisHandler;
import main.entities.FileEntity;

import java.util.List;

public class Handler {
    private DbHandler redis;
    private DbHandler mongoDB;

    Handler() {
        try {
            redis = new RedisHandler();
            redis.initDatabaseHandler();
            mongoDB = new MongoDBHandler();
            mongoDB.initDatabaseHandler();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Handler h = new Handler();

        byte[] cont = new byte[100];
        String test1 = "buyiwfwoufwiuwfoeiofuifefe";
        String test2 = "dshjhfdyuyu";
//         h.mongoDB.put("nrupa.chitley@sjsu.edu", new FileEntity("test", "txt", test2));
//        h.mongoDB.put("nrupa.chitley@sjsu.edu", new FileEntity("test1", "txt", test1));
//        FileEntity reqFile = h.mongoDB.get("nrupa.chitley@sjsu.edu", "test1.txt");
//        System.out.println("File: " + reqFile.getFileContents());
//        List<FileEntity> ans = ((MongoDBHandler)h.mongoDB).get("nrupa.chitley@sjsu.edu");
//        System.out.println("Ans: " + ans);
//        h.mongoDB.remove("nrupa.chitley@sjsu.edu", "test1.txt");
        h.mongoDB.update("nrupa.chitley@sjsu.edu", new FileEntity("test.txt", "Hello San Francisco!!!!!!!"));
    }
}
