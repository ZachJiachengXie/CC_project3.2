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
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.excludeId;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Objects;

import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.ArrayList;



/**
 * In this task you will populate a user's timeline.
 * This task helps you understand the concept of fan-out and caching.
 * Practice writing complex fan-out queries that span multiple databases.
 * Also practice using caching mechanism to boost your backend!
 *
 * Task 5 (1):
 * Get the name and profile of the user as you did in Task 1
 * Put them as fields in the result JSON object
 *
 * Task 5 (2);
 * Get the follower name and profiles as you did in Task 2
 * Put them in the result JSON object as one array
 *
 * Task 5 (3):
 * From the user's followees, get the 30 most popular comments
 * and put them in the result JSON object as one JSON array.
 * (Remember to find their parent and grandparent)
 *
 * Task 5 (4):
 * Make sure your implementation can finish a request that is sent
 * before in a short time.
 *
 * The posts should be sorted:
 * First by ups in descending order.
 * Break tie by the timestamp in descending order.
 */
public class TimelineWithCacheServlet extends HttpServlet {

    /**
     * You need to use this variable to implement your caching
     * mechanism. Please see {@link Cache#put}, {@link Cache#get}.
     *
     */
    private static Cache cache = new Cache();
    private TimelineServlet worker;

    /**
     * Your initialization code goes here.
     */
    public TimelineWithCacheServlet() throws ClassNotFoundException, SQLException {
        worker = new TimelineServlet();
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

        // DON'T modify this method
        String id = request.getParameter("id");
        String result = getTimeline(id);

        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        response.addHeader("CacheHit", String.valueOf(cache.get(id) != null));
        PrintWriter writer = response.getWriter();
        writer.print(result);
        writer.close();
    }

    /**
     * Method to get given user's timeline.
     * You are required to implement caching mechanism with
     * given cache variable.
     *
     * @param id user id
     * @return timeline of this user
     */
    private String getTimeline(String id) {
        // TODO: implement this method
        String resultStr = "";
        resultStr = cache.get(id);
        if (resultStr == null || resultStr.isEmpty()) {
            // Cache miss
            resultStr = worker.getTimeline(id);
            JsonObject result = new JsonParser().parse(resultStr).getAsJsonObject();
            JsonArray followers = result.get("followers").getAsJsonArray();
            if (followers.size() >= 300) {
                cache.put(id, resultStr);
            }
            return resultStr;
        } else {
            return resultStr;
        }
    }
}

