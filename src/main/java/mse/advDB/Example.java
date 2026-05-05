package mse.advDB;

import org.neo4j.driver.*;

import java.io.*;
import java.net.URL;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.neo4j.driver.Values.parameters;

public class Example {

    static final int BATCH_SIZE = 200;

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();
        String startTimeStr = new java.util.Date(startTime).toString();

        String jsonPath = System.getenv("JSON_FILE");
        String neo4jIP  = System.getenv("NEO4J_IP");
        int maxNodes    = Integer.parseInt(System.getenv().getOrDefault("MAX_NODES", "50000"));

        System.out.println("JSON_FILE = " + jsonPath);
        System.out.println("NEO4J_IP  = " + neo4jIP);
        System.out.println("MAX_NODES = " + maxNodes);
        System.out.println("START_TIME = " + startTimeStr);

        Driver driver = GraphDatabase.driver(
                "bolt://" + neo4jIP + ":7687",
                AuthTokens.basic("neo4j", "test"),
                Config.builder()
                        .withMaxConnectionLifetime(30, java.util.concurrent.TimeUnit.MINUTES)
                        .withConnectionAcquisitionTimeout(2, java.util.concurrent.TimeUnit.MINUTES)
                        .build()
        );


        boolean connected = false;
        while (!connected) {
            try {
                System.out.println("Waiting Neo4j...");
                Thread.sleep(3000);
                driver.verifyConnectivity();
                connected = true;
                System.out.println("Neo4j connected!");
            } catch (Exception e) {
                System.out.println("Neo4j not ready yet...");
            }
        }

        // Index pour accélérer les MERGE
        try (Session s = driver.session()) {
            s.run("CREATE INDEX article_id IF NOT EXISTS FOR (a:ARTICLE) ON (a.id)");
            s.run("CREATE INDEX author_id IF NOT EXISTS FOR (a:AUTHOR) ON (a.id)");
            System.out.println("Indexes created.");
        }

        ObjectMapper mapper = new ObjectMapper();

        List<Map<String, Object>> articleBatch  = new ArrayList<>();
        List<Map<String, Object>> authorBatch   = new ArrayList<>();
        List<Map<String, Object>> authoredBatch = new ArrayList<>();
        List<Map<String, Object>> citesBatch    = new ArrayList<>();

        int count       = 0;
        int totalAuthors = 0;

        BufferedReader br = openStream(jsonPath);
        Session session   = driver.session();

        try {
            String line;
            while ((line = br.readLine()) != null && count < maxNodes) {

                JsonNode json;
                try {
                    json = mapper.readTree(line);
                } catch (Exception e) {
                    System.err.println("Skipped invalid JSON at line " + (count + 1));
                    count++;
                    continue;
                }

                String articleId = json.has("_id") ? json.get("_id").asText() : json.get("id").asText();
                String title     = json.has("title") ? json.get("title").asText() : "";

                Map<String, Object> articleMap = new HashMap<>();
                articleMap.put("id", articleId);
                articleMap.put("title", title);
                articleBatch.add(articleMap);

                if (json.has("authors")) {
                    for (JsonNode a : json.get("authors")) {
                        String aid  = (a.has("id") && !a.get("id").asText().isEmpty())
                                ? a.get("id").asText() : a.get("name").asText();
                        String name = a.has("name") ? a.get("name").asText() : "";

                        Map<String, Object> authorMap = new HashMap<>();
                        authorMap.put("id", aid);
                        authorMap.put("name", name);
                        authorBatch.add(authorMap);
                        totalAuthors++;

                        Map<String, Object> relMap = new HashMap<>();
                        relMap.put("aid", aid);
                        relMap.put("pid", articleId);
                        authoredBatch.add(relMap);
                    }
                }

                if (json.has("references")) {
                    for (JsonNode r : json.get("references")) {
                        Map<String, Object> citeMap = new HashMap<>();
                        citeMap.put("from", articleId);
                        citeMap.put("to", r.asText());
                        citesBatch.add(citeMap);
                    }
                }

                count++;

                if (articleBatch.size() >= BATCH_SIZE) {
                    // Renouvelle la session tous les 10 batchs
                    if (count % (BATCH_SIZE * 10) == 0) {
                        session.close();
                        session = driver.session();
                    }
                    flushBatch(session, articleBatch, authorBatch, authoredBatch, citesBatch);
                    System.out.println("Flushed at: " + count);
                    articleBatch.clear();
                    authorBatch.clear();
                    authoredBatch.clear();
                    citesBatch.clear();
                }
            }

            // ← flush du dernier batch
            if (!articleBatch.isEmpty()) {
                flushBatch(session, articleBatch, authorBatch, authoredBatch, citesBatch);
                System.out.println("Flushed final batch at: " + count);
            }

        } finally {
            br.close();
            session.close();
        }

        long endTime    = System.currentTimeMillis();
        long durationMs = endTime - startTime;

        System.out.println("=== LOADING COMPLETE ===");
        System.out.println("START_TIME   = " + startTimeStr);
        System.out.println("END_TIME     = " + new java.util.Date(endTime).toString());
        System.out.println("DURATION (s) = " + durationMs / 1000);
        System.out.println("TOTAL ARTICLES = " + count);
        System.out.println("TOTAL AUTHORS  = " + totalAuthors);
        System.out.println("TOTAL NODES    = " + (count + totalAuthors));

        driver.close();
    }


    private static BufferedReader openStream(String path) throws Exception {
        if (path.startsWith("http")) {
            System.out.println("Streaming from URL...");
            URL url = new URL(path);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setReadTimeout(0);
            conn.setConnectTimeout(30_000);
            return new BufferedReader(new InputStreamReader(conn.getInputStream()));
        } else {
            System.out.println("Reading local file...");
            return new BufferedReader(new FileReader(path));
        }
    }

    private static void flushBatch(Session session,
                                   List<Map<String, Object>> articles,
                                   List<Map<String, Object>> authors,
                                   List<Map<String, Object>> authored,
                                   List<Map<String, Object>> cites) {

        final List<Map<String, Object>> a  = new ArrayList<>(articles);
        final List<Map<String, Object>> au = new ArrayList<>(authors);
        final List<Map<String, Object>> re = new ArrayList<>(authored);
        final List<Map<String, Object>> ci = new ArrayList<>(cites);

        session.writeTransaction(tx -> {
            tx.run("UNWIND $batch AS row MERGE (a:ARTICLE {id: row.id}) SET a.title = row.title",
                    parameters("batch", a));
            if (!au.isEmpty())
                tx.run("UNWIND $batch AS row MERGE (a:AUTHOR {id: row.id}) SET a.name = row.name",
                        parameters("batch", au));
            if (!re.isEmpty())
                tx.run("UNWIND $batch AS r MATCH (a:AUTHOR {id: r.aid}) MATCH (p:ARTICLE {id: r.pid}) MERGE (a)-[:AUTHORED]->(p)",
                        parameters("batch", re));
            if (!ci.isEmpty())
                tx.run("UNWIND $batch AS r MERGE (a:ARTICLE {id: r.from}) MERGE (b:ARTICLE {id: r.to}) MERGE (a)-[:CITES]->(b)",
                        parameters("batch", ci));
            return null;
        });
    }
}