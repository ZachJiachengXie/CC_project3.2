package edu.cmu.cc.minisite;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Objects;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

/**
 * Task 2:
 * Implement your logic to retrieve the followers of this user.
 * You need to send back the Name and Profile Image URL of his/her Followers.
 *
 * You should sort the followers alphabetically in ascending order by Name.
 */
public class FollowerServlet extends HttpServlet {

    /**
     * The Neo4j driver.
     */
    private final Driver driver;

    /**
     * The endpoint of the database.
     *
     * To avoid hardcoding credentials, use environment variables to include
     * the credentials.
     *
     * e.g., before running "mvn clean package exec:java" to start the server
     * run the following commands to set the environment variables.
     * export NEO4J_HOST=...
     * export NEO4J_NAME=...
     * export NEO4J_PWD=...
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
     * Initialize the connection.
     */
    public FollowerServlet() {
        Objects.requireNonNull(NEO4J_HOST);
        Objects.requireNonNull(NEO4J_NAME);
        Objects.requireNonNull(NEO4J_PWD);
        driver = getDriver();
    }

    /**
     * Constructor for mocking the class behaviour
     * @param driver    Mocked driver object
     */
    FollowerServlet(Driver driver) {
        this.driver = driver;
    }

    private Driver getDriver() {
        Objects.requireNonNull(NEO4J_HOST);
        Objects.requireNonNull(NEO4J_NAME);
        Objects.requireNonNull(NEO4J_PWD);
        return GraphDatabase.driver(
                "bolt://" + NEO4J_HOST + ":7687",
                AuthTokens.basic(NEO4J_NAME, NEO4J_PWD));
    }


    /**
     * Method to get the user UD from the request,
     * and print the response.
     * @param request  the request object that is passed to the servlet
     * @param response the response object that the servlet
     *                 uses to return the headers to the client
     * @throws IOException      if an input or output error occurs
     * @throws ServletException if the request for the HEAD
     *                          could not be handled
     */
    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        String id = request.getParameter("id");
        JsonObject result = new JsonObject();
        result.add("followers", getFollowers(id));
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.write(result.toString());
        writer.close();
    }


    /**
     * Return the name and profile image url of followers, sorted
     * lexicographically in ascending order by userName.
     * Input: id(string)
     * Output: [{name, url}...]
     */
    public JsonArray getFollowers(String id) {
        JsonArray followers = new JsonArray();
        // TODO: To be implemented
        return followers;
    }
}


