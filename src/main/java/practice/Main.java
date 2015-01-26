package practice;

import com.mongodb.*;
import java.util.*;
import java.net.UnknownHostException;





class Main {

    private static final String HOST = "localhost";
    private static final int PORT = 27017;
    private static final String USER = "bob";
    private static final char[] PASSWORD = "p@ssw0rd".toCharArray();
    private static final String DATABASE = "mydatabase";

    private static List<DBObject> queryMongo(String collection, Map query, Map fields) throws Throwable {
        ServerAddress serverAddress = new ServerAddress(HOST,PORT);
        MongoCredential mongoCredential = MongoCredential.createMongoCRCredential(USER,DATABASE,PASSWORD);
        MongoClient mongoClient = new MongoClient(serverAddress, Arrays.asList(mongoCredential));
        DB db = mongoClient.getDB(DATABASE);
        DBCollection dbCollection = db.getCollection(collection);

        List<DBObject> results = new ArrayList<DBObject>();
        try {
            DBCursor cursor = dbCollection.find(new BasicDBObject(query),new BasicDBObject(fields));
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        } catch (Throwable throwable) {
            throw throwable;
        } finally {
            mongoClient.close();
        }
        return results;
    }


    public static void main(String[] args) throws Throwable {
        System.out.println("######################################################");
        System.out.println("Connecting to "+HOST+" on port "+PORT+" as user "+USER);
        Map query = new HashMap();
        query.put("states.state","Running");
        Map fields = new HashMap();
        fields.put("name",1);
        List<DBObject> results = queryMongo("job",query,fields);
        System.out.println("read in "+results.size()+" jobs");
        for (DBObject dbObj : results) {
            System.out.println(dbObj);
        }
    }



}
