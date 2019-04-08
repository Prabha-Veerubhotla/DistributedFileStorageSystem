package main.db;

import grpc.route.server.RouteServerImpl;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RedisHandlerTest {
    RedisHandler rh = new RedisHandler();
    @Test
    public void put() {
        String content1 = "How are you?";
        String content2 ="I am fine.";
        byte[] cont1 = content1.getBytes();
        byte[] cont2 = content2.getBytes();
        String userName = "Nrupa";
        String fileName = "test.txt";
        String fileName1 = "test1.txt";
        Map<String, byte[]> actualRes = new HashMap<>();
        rh.put(userName, fileName, "1", cont1);
        actualRes = rh.get(userName, fileName);
        assertEquals(content1, new String(actualRes.get("1")));
        rh.put(userName, fileName, "2", cont2);
        actualRes = rh.get(userName, fileName);
        byte[] actRes = RouteServerImpl.combineBytes(actualRes);
        String expectedRes = content1.concat(content2);
        assertEquals(expectedRes, new String(actRes));
        rh.put(userName, fileName1, "1", cont1);
        actualRes = rh.get(userName, fileName1);
        assertEquals(content1, new String(actualRes.get("1")));
    }

    @Test
    public void get() {
        String userName1 = "Nrupa";
        String userName2 = "N";
        String fileName = "test2.txt";
        Map<String, byte[]> actualRes = new HashMap<>();
        actualRes = rh.get(userName1, fileName);
        assertEquals(null, actualRes);
        actualRes = rh.get(userName2, fileName);
        assertEquals(null, actualRes);
    }

    @Test
    public void remove() {
        String userName = "Nrupa";
        String fileName = "test1.txt";
        boolean actualRes  = rh.remove(userName, fileName);
        assertEquals(true, actualRes);
    }

    @Test
    public void update() {
        String userName = "Nrupa";
        String fileName = "test.txt";
        String content = "Updated Content!!!!";
        byte[] contentByte = content.getBytes();
        rh.update(userName, fileName, "1", contentByte);
        Map<String, byte[]> actualRes = new HashMap<>();
        actualRes = rh.get(userName, fileName);
        assertEquals(content, new String(actualRes.get("1")));
    }
}