package edu.cmu.cc.minisite;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import java.io.IOException;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import javax.servlet.ServletException;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import static com.mongodb.client.model.Projections.excludeId;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Objects;

/**
 * In this task you will populate a user's timeline.
 * This task helps you understand the concept of fan-out. 
 * Practice writing complex fan-out queries that span multiple databases.
 *
 * Task 4 (1):
 * Get the name and profile of the user as you did in Task 1
 * Put them as fields in the result JSON object
 *
 * Task 4 (2);
 * Get the follower name and profiles as you did in Task 2
 * Put them in the result JSON object as one array
 *
 * Task 4 (3):
 * From the user's followees, get the 30 most popular comments
 * and put them in the result JSON object as one JSON array.
 * (Remember to find their parent and grandparent)
 *
 * The posts should be sorted:
 * First by ups in descending order.
 * Break tie by the timestamp in descending order.
 */
public class TimelineServlet extends HttpServlet {
    /***********************************************************************************
     * MySQL setup
     ***********************************************************************************/
    /**
     * JDBC driver of MySQL Connector/J.
     */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    /**
     * Database name.
     */
    private static final String REDDIT_DB_NAME = "reddit_db";

    /**
     * The endpoint of the sql database.
     */
    private static String mysqlHost = System.getenv("MYSQL_HOST");

    /**
     * MySQL username.
     */
    private static String mysqlName = System.getenv("MYSQL_NAME");

    /**
     * MySQL Password.
     */
    private static String mysqlPwd = System.getenv("MYSQL_PWD");

    /**
     * Reddit MySQL DB connection
     */
    private static Connection redditSQLConn;

    /**
     * MySQL URL.
     */
    private static final String MYSQLURL = "jdbc:mysql://" + mysqlHost + ":3306/"
            + REDDIT_DB_NAME + "?useSSL=false";


    /***********************************************************************************
     * MongoDB setup
     ***********************************************************************************/
    /**
     * The endpoint of the MongoDB database.
     */
    private static final String MONGO_HOST = System.getenv("MONGO_HOST");
    /**
     * MongoDB server URL.
     */
    private static final String MONGOURL = "mongodb://" + MONGO_HOST + ":27017";

    /**
     * Collection name.
     */
    private static final String COLLECTION_NAME = "posts";
    /**
     * MongoDB connection.
     */
    private static MongoCollection<Document> collection;


    /***********************************************************************************
     * Neo4j setup
     ***********************************************************************************/
    /**
     * The Neo4j driver.
     */
    private final Driver driver;

    /**
     * The endpoint of the neo4j database.
     */
    private static final String NEO4J_HOST = System.getenv("NEO4J_HOST");
    /**
     * Neo4J username.
     */
    private static final String NEO4J_NAME = System.getenv("NEO4J_NAME");
    /**
     * Neo4J Password.
     */
    private static final String NEO4J_PWD = System.getenv("NEO4J_PWD");

    private static final JsonObject EMPTY_JSONOBJ = new JsonObject();


    /**
     * Your initialization code goes here.
     */
    public TimelineServlet() throws ClassNotFoundException, SQLException {
        redditSQLConn = getRedditDBMySQLConnection();
        collection = setupMongoConnection();
        driver = getDriver();
    }

    /**
     * Initialize the MongoDB connection.
     */
    private MongoCollection<Document> setupMongoConnection() {
        Objects.requireNonNull(MONGO_HOST);
        MongoClientURI connectionString = new MongoClientURI(MONGOURL);
        MongoClient mongoClient = new MongoClient(connectionString);
        MongoDatabase database = mongoClient.getDatabase(REDDIT_DB_NAME);
        return database.getCollection(COLLECTION_NAME);
    }

    /**
     * Initialize the MYSql connection.
     */
    private Connection getRedditDBMySQLConnection() throws SQLException {
        Objects.requireNonNull(mysqlHost);
        Objects.requireNonNull(mysqlName);
        Objects.requireNonNull(mysqlPwd);
        return DriverManager.getConnection(MYSQLURL, mysqlName, mysqlPwd);
    }

    /**
     * Initialize the neo4j driver.
     */
    private Driver getDriver() {
        Objects.requireNonNull(NEO4J_HOST);
        Objects.requireNonNull(NEO4J_NAME);
        Objects.requireNonNull(NEO4J_PWD);
        return GraphDatabase.driver(
                "bolt://" + NEO4J_HOST + ":7687",
                AuthTokens.basic(NEO4J_NAME, NEO4J_PWD));
    }

    /**
     * Don't modify this method.
     *
     * @param request  the request object that is passed to the servlet
     * @param response the response object that the servlet
     *                 uses to return the headers to the client
     * @throws IOException      if an input or output error occurs
     * @throws ServletException if the request for the HEAD
     *                          could not be handled
     */
    @Override
    protected void doGet(final HttpServletRequest request,
                         final HttpServletResponse response) throws ServletException, IOException {

        // DON'T modify this method.
        String id = request.getParameter("id");
        String result = getTimeline(id);
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.print(result);
        writer.close();
    }


    /**
     * Method to get given user's timeline.
     *
     * @param id user id
     * @return timeline of this user
     */
    private String getTimeline(String id)  {
        JsonObject result = new JsonObject();
        // TODO: implement this method
        try {
            JsonObject user = getUser(id);
            if (user.equals(EMPTY_JSONOBJ)) {
                // user does not exit, empty response.
                return result.toString();
            }
            result = user;
            //  Get followers
            result.add("followers", getFollowers(id));
            //  Get followees
            JsonArray myFollowees = getFollowees(id);
            JsonArray comments = new JsonArray();
            for (JsonElement followeeElem : myFollowees) {
                JsonObject followee = followeeElem.getAsJsonObject();
                System.out.println("!!!followee: !!!" + followee.toString());
                comments.addAll(proccessHotCommentsForUser(followee.get("name").getAsString(), 30));
            }
            result.add("comments", comments);
        } catch (SQLException e){
            e.printStackTrace();
        }
        return result.toString();
    }


    /**
     * Method to perform the SQL query, retrieve the results and
     * construct and return a JsonObject with the expected result
     *
     * @param name  The username supplied via the HttpServletRequest
     * @return A JsonObject with the servlet's response
     */
    public JsonObject getUser(String name) throws SQLException {
        JsonObject result = new JsonObject();
        String query = "SELECT * FROM users WHERE username = ?";
        PreparedStatement pstmt = redditSQLConn.prepareStatement(query);
        pstmt.setString(1, name);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            result.addProperty("name", rs.getString("username"));
            result.addProperty("profile", rs.getString("profile_photo_url"));
        } 
        return result;
    }

    public JsonArray getFollowers(String followeeId) {
        JsonArray followers = new JsonArray();
        String query = "MATCH (follower:User)-[r:FOLLOWS]->(followee:User {username: \"" + followeeId + "\"}) RETURN follower.username, follower.url ORDER BY follower.username";
        try (Session session = driver.session())
        {
            StatementResult result = session.run(query);
            while (result.hasNext()) {
                Record record = result.next();
                JsonObject link = new JsonObject();
                link.addProperty("name", record.get(0).asString());
                link.addProperty("profile", record.get(1).asString());
                followers.add(link);
            }
        }
        return followers;
    }

    public JsonArray getFollowees(String followerId) {
        JsonArray followees = new JsonArray();
        String query = "MATCH (follower:User {username: \"" + followerId + "\"})-[r:FOLLOWS]->(followee:User) RETURN followee.username, followee.url ORDER BY followee.username";
        try (Session session = driver.session())
        {
            StatementResult result = session.run(query);
            while (result.hasNext()) {
                Record record = result.next();
                JsonObject link = new JsonObject();
                link.addProperty("name", record.get(0).asString());
                link.addProperty("profile", record.get(1).asString());
                followees.add(link);
            }
        }
        return followees;
    }

    public JsonArray proccessHotCommentsForUser(String userId, int limit) {
        JsonArray result = new JsonArray();
        JsonArray baseComments = getCommentsByUid(userId, limit);
        for (JsonElement childCommentElem : baseComments) {
            JsonObject childComment = childCommentElem.getAsJsonObject();
            String parentId = childComment.get("parent_id").getAsString();
            JsonObject parentComment = getCommentByCid(parentId);

            if (parentComment.equals(EMPTY_JSONOBJ)) {
                // If cannot find parent in the given data, only add child and continue
                
            } else {
                // If parent is present, add parent and proceed to find grandparnet.
                childComment.add("parent", parentComment);
                String grandParentId = parentComment.get("parent_id").getAsString();

                JsonObject grandParentComment = getCommentByCid(grandParentId);
                if (!grandParentComment.equals(EMPTY_JSONOBJ)) {
                    childComment.add("grand_parent", grandParentComment);
                }
            }
            result.add(childComment);
        }
        return result;
    }

    public JsonArray getCommentsByUid(String uid, int limit) {
        JsonArray result = new JsonArray();
        Bson find_filter = eq("uid", uid);
        Bson sort_filter = orderBy(descending("ups"), descending("timestamp"));
        MongoCursor<Document> cursor = collection.find(find_filter).sort(sort_filter).projection(excludeId()).limit(limit).iterator();
        try {
           while (cursor.hasNext()) {
                JsonObject jsonObject = new JsonParser().parse(cursor.next().toJson()).getAsJsonObject();
                result.add(jsonObject);
            }
        } finally {
            cursor.close();
        }
        return result;
    }

    public JsonObject getCommentByCid(String cid) {
        JsonObject result = new JsonObject();

        if(cid == null || cid.isEmpty()) {
            // cid is empty
            return result;
        }
        Bson find_filter = eq("cid", cid);
        MongoCursor<Document> cursor = collection.find(find_filter).projection(excludeId()).iterator();
        try {
           while (cursor.hasNext()) {
                result = new JsonParser().parse(cursor.next().toJson()).getAsJsonObject();
            }
        } finally {
            cursor.close();
        }
        return result;
    }
}

