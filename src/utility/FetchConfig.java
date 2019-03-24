package utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FetchConfig {
    protected static Logger logger = LoggerFactory.getLogger("Fetch-Config");
    private static List<String> msgTypes = Arrays.asList("join", "get", "put", "list", "delete", "request-ip", "save-client-ip", "slave-ip");


    public static Properties getConfiguration(final File path) throws IOException {
        if (!path.exists())
            throw new IOException("missing file");

        Properties rtn = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(path);
            rtn.load(fis);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    logger.info("Unable to read from config file: " + path + " due to exception: " + e);
                }
            }
        }
        return rtn;
    }

    public static List<String> getMsgTypes() {
        return msgTypes;
    }

    public byte[] convertFileContent(String filename) {
        RandomAccessFile f;
        try {
            try {
                f = new RandomAccessFile(filename, "r");
            } catch (FileNotFoundException fe) {
                //if file is not found
                // if it is msg, return byte array
                logger.info("File: " + filename + " not found");
                logger.info("Saving it as message");
                return filename.getBytes();
            }
            logger.info("file length is: " + f.length());
            byte[] b = new byte[(int) f.length()];
            f.readFully(b);
            f.close();
            return b;
        } catch (IOException ie) {
            logger.info("Unable to convert file contents of: " + filename + " to byte array");
        }
        return new byte[1]; // return byte array of size 1, if unable to read file content
    }

}
