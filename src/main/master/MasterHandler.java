package main.master;

import java.util.List;
import java.util.logging.Logger;

public class MasterHandler {
    Logger logger = Logger.getLogger(MasterHandler.class.getName());

    public void createFile(String fileName, String filePath, String email){
        ReadWrite rw = new ReadWrite();
        List<byte[]> out = rw.convertFileToByteArray(fileName, filePath);
        logger.info("Out-> " + out);
        rw.convertByteArrayToFile("Output.jpeg", out);
    }

    public void fileHandler(String task, String fileName, String filePath, String email){
        MasterHandler mn = new MasterHandler();
        logger.info("HERE");
        switch (task){
            case "CREATE FILE":
                mn.createFile(fileName, filePath, email);
                break;

        }

    }

    public static void main(String[] args){
        MasterHandler m = new MasterHandler();
        m.fileHandler("CREATE FILE", "Test.txt", "/Users/nrupa/Desktop/WhatsApp Image 2019-03-06 at 8.50.09 AM.jpeg", "test@gmail.com");
    }
}
