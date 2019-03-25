package grpc.route.server;

import com.google.rpc.Help;
import main.db.RedisHandler;

import java.util.*;
import java.util.logging.Logger;

@SuppressWarnings("unchecked")
public class MasterMetaData {
    Logger logger = Logger.getLogger(MasterMetaData.class.getName());
    //{userName: {fileName : {IP : [shards]}}}
    Map<String, Map<String, Map<String, List<String>>>> MetaData = new HashMap<>();

    public void putMetaData(String userName, String fileName, String seqID, String IP) {
        if (MetaData.containsKey(userName)) {
            Map<String, Map<String, List<String>>> userMap = MetaData.get(userName);
            if (userMap.containsKey(fileName)) {
                Map<String, List<String>> IPShardMap = userMap.get(fileName);
                if (!IPShardMap.containsKey(IP)) {
                    List<String> t1 = new ArrayList<>();
                    IPShardMap.put(IP, t1);
                }
                List<String> st = IPShardMap.get(IP);
                st.add(seqID);
            } else {
                List<String> t = new ArrayList();
                t.add(seqID);
                Map<String, List<String>> map = new HashMap<>();
                map.put(IP, t);
                userMap.put(fileName, map);
            }
            logger.info("Success " + userName);
        } else {
            Map<String, List<String>> innerMap = new HashMap<>();
            List<String> shardID = new ArrayList<>();
            shardID.add(seqID);
            innerMap.put(IP, shardID);
            Map<String, Map<String, List<String>>> newMap = new HashMap<>();
            newMap.put(fileName, innerMap);
            logger.info("Map-> " + newMap);
            MetaData.put(userName, newMap);
            logger.info("Success " + userName);
        }
    }

    @SuppressWarnings("unchecked")
    public List<String> getMetaData(String userName, String fileName, String IP) {
        return MetaData.getOrDefault(userName,
                new HashMap<>()).getOrDefault(fileName,
                new HashMap<>()).getOrDefault(IP,
                new ArrayList<>());
    }

    /**
     * Get all files for a user from MetaData
     *
     * @param userName
     * @return
     */
    @SuppressWarnings("unchecked")
    public Set<String> getAllFiles(String userName) {
        if (MetaData.containsKey(userName)) {
            Map<String, Map<String, List<String>>> allFiles = MetaData.get(userName);
            logger.info("All user Files: " + allFiles);
            return allFiles.keySet();
        } else {
            logger.info("User not present");
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public void deleteFileFormMetaData(String userName, String fileName) {
        Map<String, Map<String, List<String>>> allUserFiles = MetaData.getOrDefault(userName, new HashMap<>());
        allUserFiles.remove(fileName);
    }

    //TODO: To be implemented
    public void updateMetaData(){

    }

//    public static void main(String[] args){
//        String filePath = "/Users/nrupa/Desktop/WhatsApp Image 2019-03-06 at 8.50.09 AM.jpeg";
//        String filePath1 = "/Users/nrupa/Desktop/ChitleyNrupaIntervieweeInformation.docx";
//        String payload = null;
//        try {
//            RandomAccessFile f = new RandomAccessFile(filePath, "r");
//            byte[] b = new byte[(int) f.length()];
//            f.readFully(b);
//            f.close();
//            payload = new String(b);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
////        masterMetadata.taskHandler("PUT", "report.docx", payload, "test@gmail.com");
////        masterMetadata.taskHandler("PUT", "pic.jpeg", payload, "test@gmail.com");
//        masterMetadata.taskHandler("GET", "pic.jpeg",null, "test@gmail.com");
////        masterMetadata.taskHandler("DELETE", "report.docx", null, "test@gmail.com");
////        masterMetadata.taskHandler("GET ALL FILES", null, null, "test@gmail.com");
//    }
//}

}
