package aggregation;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.BulkWriteOptions;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

public class BulkMongoClient {

    private MongoClient client;
    private MongoCollection<Document> collection;

    public BulkMongoClient(String url, String collectionName) {
        //Creates a MongoURI from the given string.
        MongoClientURI uri = new MongoClientURI(url);
        //Creates a MongoClient described by a URI.
        this.client = new MongoClient(uri);
        //Gets a Database.
        MongoDatabase db = client.getDatabase(uri.getDatabase());
        //Gets a collection.
        this.collection = db.getCollection(collectionName);
    }

    public void batchInsert(List< InsertOneModel<Document> > inserts) {
        if(inserts.isEmpty()) { return; } // crashes otherwise

        collection.bulkWrite(
            inserts, new BulkWriteOptions().ordered(false)
        );
    }

    public void batchUpdate(List< UpdateOneModel<Document> > updates) {
        if(updates.isEmpty()) { return; } // crashes otherwise

        collection.bulkWrite(
            updates, new BulkWriteOptions().ordered(false)
        );
    }
    
    public void close() {
        client.close();
    }

}
