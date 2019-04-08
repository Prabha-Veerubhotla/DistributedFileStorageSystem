package main.db;

import main.entities.FileEntity;
import org.bson.BSON;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MongoDBHandlerTest {
    MongoDBHandler mh = new MongoDBHandler();

    @Test
    public void put() {
        String userName = "Nrupa";
        String fileName = "test.txt";
        String content = "Hi Hello!!";
        byte[] contentByte = content.getBytes();
        Map<String, byte[]> t = new HashMap<>();
        t.put("1", contentByte);
        mh.put(userName, new FileEntity(fileName, t));
        FileEntity file = mh.get(userName, fileName);

        Document doc = (Document)file.getFileContents();
        Binary actualRes =  (Binary)doc.get("1");

        // Finding out type of object
        Class cls = actualRes.getData().getClass();
        System.out.println("The type of the object is: " + cls.getName());

        assertEquals(content, new String(actualRes.getData()));
    }

    @Test
    public void get() {
        String userName = "Nrupa";
        String fileName = "test1.txt";
        FileEntity file =  mh.get(userName, fileName);

        assertEquals(null, file);
    }

    @Test
    public void remove() {
        String userName = "Nrupa";
        String fileName = "test.txt";
        mh.remove(userName, fileName);
        FileEntity file = mh.get(userName, fileName);
        assertEquals(null, file);
    }

    @Test
    public void update() {
        String userName = "Nrupa";
        String fileName = "test.txt";
        String content = "Hi Hello!!";
        byte[] contentByte = content.getBytes();
        Map<String, byte[]> t = new HashMap<>();
        t.put("1", contentByte);
        mh.put(userName, new FileEntity(fileName, t));
        String updateContent = "Update Content!!!";
        byte[] updateContentByte = updateContent.getBytes();
        Map<String, byte[]> t1 = new HashMap<>();
        t1.put("1", updateContentByte);
        mh.update(userName, new FileEntity(fileName, t1));
        FileEntity file = mh.get(userName, fileName);

        Document doc = (Document)file.getFileContents();
        Binary actualRes =  (Binary)doc.get("1");
        assertEquals(updateContent, new String(actualRes.getData()));
    }
}