package grpc.route.server;

import org.junit.Test;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RouteServerImplTest {

    @Test
    public void combineBytes() {
        String expectedRes = "Hi How are you?";
        byte[] totalBytes = expectedRes.getBytes();
        byte[] content1 = Arrays.copyOfRange(totalBytes, 0, totalBytes.length / 2);
        byte[] content2 = Arrays.copyOfRange(totalBytes, totalBytes.length / 2, totalBytes.length);
        Map<String, byte[]> t = new HashMap<>();
        t.put("1", content1);
        t.put("2", content2);
        byte[] actualRes = RouteServerImpl.combineBytes(t);
        assertEquals(expectedRes, new String(actualRes));
    }
}