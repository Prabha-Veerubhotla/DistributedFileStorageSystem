package main.db;


import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.sun.istack.NotNull;
import main.entities.FileEntity;
import org.bson.Document;
import com.mongodb.client.MongoCollection;
import java.util.*;

public class MongoDBHandler implements DbHandler {
    private  MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    @Override
    public void initDatabaseHandler() throws Exception {

        logger.info("initDatabaseHandler");
        String databaseName = "fluffy";

        try {
            mongoClient = new MongoClient();
            database = mongoClient.getDatabase(databaseName);
            collection = database.getCollection("Files");
            logger.info("Successful connection!");
        }
        catch (Exception e) {
            throw e;
        }
    }

    @Override
    public String put(String userEmail, FileEntity file) {
        logger.info("userEmail: " + userEmail);
        logger.info("file: " + file.getFileContents());
        try {
            BasicDBObject findQuery = new BasicDBObject("personEmail", userEmail);
            FindIterable<Document> temp = collection.find(findQuery);
            logger.info("Checking if data exists");
            if(temp.first() != null){
                logger.info("Inside if ");
                BasicDBObject listItem = new BasicDBObject("allData", new BasicDBObject("fileName", file.toString()).append("value",file.getFileContents()));
                BasicDBObject updateQuery = new BasicDBObject("$push", listItem);
                collection.updateOne(findQuery, updateQuery);
            }
            else {
                List<BasicDBObject> allData = new ArrayList<>();
                allData.add(new BasicDBObject("fileName", file.toString()).append("value", file.getFileContents()));
                Document doc = new Document("personEmail", userEmail)
                        .append("allData", allData);
                collection.insertOne(doc);
                logger.info("Success " + userEmail);
            }
        } catch (Exception ex) {
        }
        return file.getFileName();
    }

    @Override
    public FileEntity get(@NotNull String email, @NotNull String fileName){
        try {
            BasicDBObject elementQuery = new BasicDBObject("fileName", fileName);
            BasicDBObject query = new BasicDBObject("allData", new BasicDBObject("$elemMatch", elementQuery));
            query.put("personEmail", email);
            Document doc = collection.find(query).first();
            logger.info("Query Successful" + doc.toString());
            List<Document> dataList = (List<Document>)doc.get("allData");
            for( Document docu : dataList){
                String checkFile = (String) docu.get("fileName");
                if(checkFile.equals(fileName)){
                    logger.info("Got file: " + checkFile);
                    return new FileEntity(checkFile, docu.get("value"));
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public List<FileEntity> get(@NotNull String email){
        List<FileEntity> allFiles = new ArrayList<>();
        BasicDBObject findQuery = new BasicDBObject("personEmail", email);
        Document data = collection.find(findQuery).first();
        logger.info("data: " + data);
        List<Document> dataList = (List<Document>)data.get("allData");
        for(Document doc : dataList){
            FileEntity newFile = new FileEntity((String)doc.get("fileName"), doc.get("value"));
            allFiles.add(newFile);
        }
        return allFiles;
    }

    @Override
    public void remove(@NotNull String email, @NotNull String fileName){
        BasicDBObject query = new BasicDBObject("personEmail", email);
        BasicDBObject update = new BasicDBObject("allData", new BasicDBObject("fileName", fileName));
        collection.updateOne(query, new BasicDBObject("$pull", update));
    }

    @Override
    public FileEntity update(@NotNull String email, @NotNull FileEntity file){
        BasicDBObject query = new BasicDBObject("personEmail", email).append("allData.fileName", file.getFileName());
        BasicDBObject update = new BasicDBObject();
        update.put("allData.$.value", file.getFileContents());
        collection.updateOne(query, new BasicDBObject("$set", update));
        return file;
    }
}
