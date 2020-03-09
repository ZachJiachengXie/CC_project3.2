package edu.cmu.cc.minisite;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
    private String getTimeline(String id) {
        JsonObject result = new JsonObject();
        // TODO: implement this method
        return result.toString();
    }


    /**
     * Method to perform the SQL query, retrieve the results and
     * construct and return a JsonObject with the expected result
     *
     * @param name  The username supplied via the HttpServletRequest
     * @param pwd   The password supplied via the HttpServletRequest
     * @return A JsonObject with the servlet's response
     */
    JsonObject getUser(String name, String pwd) throws SQLException {
        JsonObject result = new JsonObject();
        String query = "SELECT * FROM users WHERE username = ? AND pwd = ?";
        System.out.println("name:" + name + " , passwd : " + pwd);
        PreparedStatement pstmt = redditSQLConn.prepareStatement(query);
        pstmt.setString(1, name);
        pstmt.setString(2, pwd);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            result.addProperty("name", rs.getString("username"));
            result.addProperty("profile", rs.getString("profile_photo_url"));
        } else {
            result.addProperty("name", "Unauthorized");
            result.addProperty("profile", "#");
        }

        return result;
    }
}

