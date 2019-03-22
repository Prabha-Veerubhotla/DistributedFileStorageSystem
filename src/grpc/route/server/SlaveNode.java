package grpc.route.server;

import com.google.protobuf.ByteString;
import main.entities.FileEntity;
import main.slave.SlaveHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import route.Route;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SlaveNode extends RouteServerImpl {
    protected static Logger logger = LoggerFactory.getLogger("server-slave");
    static Map<String, List<String>> map = new HashMap<>();
    static SlaveHandler sh = new SlaveHandler();

    public static boolean saveFile(String filename, String name, ByteString content) {
        sh.createNewFile(name, new FileEntity(filename, content));
        return true;
    }

    public boolean put(Route r) {
        String name = r.getUsername();
        String path = r.getPath();
        String payload = r.getPayload().toString();
        sh.createNewFile(name, new FileEntity(path, payload));
        return true;
    }

    public FileEntity get(Route r) {
        String payload = r.getPayload().toString();
        logger.info("retrieving information of: " + payload);
        String name = r.getUsername();
        FileEntity result = sh.retrieveFile(name, payload);
        return result;
    }

    public boolean delete(Route r) {
        boolean status = false;
        String name = r.getUsername();
        String msg = r.getPayload().toString();
        logger.info("deleting message " + msg + " from:  " + name + " in slave..");
        if (map.containsKey(name)) {
            List<String> messages = map.get(name);
            messages.remove(msg);
            map.put(name, messages);
            status = true;
        }
        return status;
    }

    public List<String> list(Route r) {
        logger.info("listing messages or files of: " + r.getUsername());
        String username = r.getUsername();
        List<String> stringList = new ArrayList<>();
        if (map.containsKey(username)) {
            return map.get(username);
        }
        return stringList;
    }
}

    // 1. put data into the database
   /* public static boolean putFilePartition(String filename) {

        return true;
    }*/


    // 2. reqtrieve data from the database and send it to server upon a request

   /* public static byte[] getFilePartition(String filename) {

        return new byte[5];
    }
*/
    // 3. talk with master node
    /*public void sendMessageToMaster() {

    }*/

    // v2: 4. maintain a in memory, cache

