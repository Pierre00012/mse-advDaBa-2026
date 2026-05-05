package mse.advDB;

import org.neo4j.driver.*;

import java.io.*;
import java.net.URL;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.neo4j.driver.Values.parameters;

public class Example {

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        String jsonPath = System.getenv("JSON_FILE");
        String neo4jIP = System.getenv("NEO4J_IP");
        int maxNodes = Integer.parseInt(System.getenv("MAX_NODES"));

        System.out.println("JSON_FILE = " + jsonPath);
        System.out.println("NEO4J_IP = " + neo4jIP);
        System.out.println("MAX_NODES = " + maxNodes);

        Driver driver = GraphDatabase.driver(
                "bolt://" + neo4jIP + ":7687",
                AuthTokens.basic("neo4j", "test")
        );

        boolean connected = false;
        while (!connected) {
            try {
                System.out.println("Waiting Neo4j...");
                Thread.sleep(3000);
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception e) {
                System.out.println("Neo4j not ready yet...");
            }
        }

        ObjectMapper mapper = new ObjectMapper();

        BufferedReader br;

        if (jsonPath.startsWith("http")) {
            System.out.println("Streaming from URL...");
            URL url = new URL(jsonPath);
            br = new BufferedReader(new InputStreamReader(url.openStream()));
        } else {
            System.out.println("Reading local file...");
            br = new BufferedReader(new FileReader(jsonPath));
        }

        List<Map<String, Object>> articleBatch = new ArrayList<>();
        List<Map<String, Object>> authorBatch = new ArrayList<>();
        List<Map<String, Object>> authoredBatch = new ArrayList<>();
        List<Map<String, Object>> citesBatch = new ArrayList<>();

        int count = 0;

        Session session = driver.session();

        try {

            String line;

            while ((line = br.readLine()) != null && count < maxNodes) {

                JsonNode json = null;
                try {
                    json = mapper.readTree(line);
                } catch (Exception e) {
                    System.err.println("Skipped invalid JSON at line " + (count + 1) + ": " + e.getMessage());
                    count++;
                    continue;
                }

                // Article
                String articleId = json.has("_id")
                        ? json.get("_id").asText()
                        : json.get("id").asText();

                String title = json.has("title") ? json.get("title").asText() : "";

                Map<String, Object> articleMap = new HashMap<>();
                articleMap.put("id", articleId);
                articleMap.put("title", title);
                articleBatch.add(articleMap);

                // Auteurs
                if (json.has("authors")) {
                    for (JsonNode a : json.get("authors")) {

                        String aid = (a.has("id") && !a.get("id").asText().isEmpty())
                                ? a.get("id").asText()
                                : a.get("name").asText();

                        String name = a.has("name") ? a.get("name").asText() : "";

                        Map<String, Object> authorMap = new HashMap<>();
                        authorMap.put("id", aid);
                        authorMap.put("name", name);
                        authorBatch.add(authorMap);

                        Map<String, Object> relMap = new HashMap<>();
                        relMap.put("aid", aid);
                        relMap.put("pid", articleId);
                        authoredBatch.add(relMap);
                    }
                }

                // Cite
                if (json.has("references")) {
                    for (JsonNode r : json.get("references")) {

                        Map<String, Object> citeMap = new HashMap<>();
                        citeMap.put("from", articleId);
                        citeMap.put("to", r.asText());
                        citesBatch.add(citeMap);
                    }
                }

                count++;

                if (articleBatch.size() >= 200) {

                    final List<Map<String, Object>> aBatch = new ArrayList<>(articleBatch);
                    final List<Map<String, Object>> auBatch = new ArrayList<>(authorBatch);
                    final List<Map<String, Object>> authBatch = new ArrayList<>(authoredBatch);
                    final List<Map<String, Object>> cBatch = new ArrayList<>(citesBatch);

                    session.writeTransaction(new TransactionWork<Void>() {
                        @Override
                        public Void execute(Transaction tx) {

                            tx.run(
                                    "UNWIND $batch AS row " +
                                            "MERGE (a:ARTICLE {id: row.id}) " +
                                            "SET a.title = row.title",
                                    parameters("batch", aBatch)
                            );

                            tx.run(
                                    "UNWIND $batch AS row " +
                                            "MERGE (a:AUTHOR {id: row.id}) " +
                                            "SET a.name = row.name",
                                    parameters("batch", auBatch)
                            );

                            tx.run(
                                    "UNWIND $batch AS r " +
                                            "MATCH (a:AUTHOR {id: r.aid}) " +
                                            "MATCH (p:ARTICLE {id: r.pid}) " +
                                            "MERGE (a)-[:AUTHORED]->(p)",
                                    parameters("batch", authBatch)
                            );

                            tx.run(
                                    "UNWIND $batch AS r " +
                                            "MERGE (a:ARTICLE {id: r.from}) " +
                                            "MERGE (b:ARTICLE {id: r.to}) " +
                                            "MERGE (a)-[:CITES]->(b)",
                                    parameters("batch", cBatch)
                            );

                            return null;
                        }
                    });

                    System.out.println("Flushed at: " + count);

                    articleBatch.clear();
                    authorBatch.clear();
                    authoredBatch.clear();
                    citesBatch.clear();
                }
            }

        } finally {
            br.close();
            session.close();
        }

        long endTime = System.currentTimeMillis();

        System.out.println("DONE");
        System.out.println("TOTAL ARTICLES = " + count);
        System.out.println("TIME (s) = " + (endTime - startTime) / 1000);

        driver.close();
    }
}