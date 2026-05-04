package mse.advDB;

import org.neo4j.driver.*;

import java.io.BufferedReader;
import java.io.FileReader;
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

        // batch
        List<Map<String, Object>> articleBatch = new ArrayList<>();
        List<Map<String, Object>> authorBatch = new ArrayList<>();
        List<Map<String, Object>> authoredBatch = new ArrayList<>();
        List<Map<String, Object>> citesBatch = new ArrayList<>();

        int count = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(jsonPath));
             Session session = driver.session()) {

            String line;

            while ((line = br.readLine()) != null && count < maxNodes) {

                JsonNode json = mapper.readTree(line);

                // Article
                String articleId = json.has("_id")
                        ? json.get("_id").asText()
                        : json.get("id").asText();

                String title = json.has("title") ? json.get("title").asText() : "";

                articleBatch.add(new HashMap<String, Object>() {{
                    put("id", articleId);
                    put("title", title);
                }});

                // autheurs
                if (json.has("authors")) {
                    for (JsonNode a : json.get("authors")) {

                        String aid = (a.has("id") && !a.get("id").asText().isEmpty())
                                ? a.get("id").asText()
                                : a.get("name").asText();

                        String name = a.has("name") ? a.get("name").asText() : "";

                        authorBatch.add(new HashMap<String, Object>() {{
                            put("id", aid);
                            put("name", name);
                        }});

                        authoredBatch.add(new HashMap<String, Object>() {{
                            put("aid", aid);
                            put("pid", articleId);
                        }});
                    }
                }

                // reference
                if (json.has("references")) {
                    for (JsonNode r : json.get("references")) {

                        citesBatch.add(new HashMap<String, Object>() {{
                            put("from", articleId);
                            put("to", r.asText());
                        }});
                    }
                }

                count++;

                if (articleBatch.size() >= 200) {

                    List<Map<String, Object>> aBatch = new ArrayList<>(articleBatch);
                    List<Map<String, Object>> auBatch = new ArrayList<>(authorBatch);
                    List<Map<String, Object>> authBatch = new ArrayList<>(authoredBatch);
                    List<Map<String, Object>> cBatch = new ArrayList<>(citesBatch);

                    session.writeTransaction(tx -> {

                        // Articles
                        tx.run(
                                "UNWIND $batch AS row " +
                                        "MERGE (a:ARTICLE {id: row.id}) " +
                                        "SET a.title = row.title",
                                parameters("batch", aBatch)
                        );

                        // Authors
                        tx.run(
                                "UNWIND $batch AS row " +
                                        "MERGE (a:AUTHOR {id: row.id}) " +
                                        "SET a.name = row.name",
                                parameters("batch", auBatch)
                        );

                        // Authored
                        tx.run(
                                "UNWIND $batch AS r " +
                                        "MATCH (a:AUTHOR {id: r.aid}) " +
                                        "MATCH (p:ARTICLE {id: r.pid}) " +
                                        "MERGE (a)-[:AUTHORED]->(p)",
                                parameters("batch", authBatch)
                        );

                        // Cites
                        tx.run(
                                "UNWIND $batch AS r " +
                                        "MERGE (a:ARTICLE {id: r.from}) " +
                                        "MERGE (b:ARTICLE {id: r.to}) " +
                                        "MERGE (a)-[:CITES]->(b)",
                                parameters("batch", cBatch)
                        );

                        return null;
                    });

                    System.out.println("Flushed at: " + count);

                    articleBatch.clear();
                    authorBatch.clear();
                    authoredBatch.clear();
                    citesBatch.clear();
                }
            }
            if (!articleBatch.isEmpty()) {

                session.writeTransaction(tx -> {

                    tx.run(
                            "UNWIND $batch AS row " +
                                    "MERGE (a:ARTICLE {id: row.id}) " +
                                    "SET a.title = row.title",
                            parameters("batch", articleBatch)
                    );

                    tx.run(
                            "UNWIND $batch AS row " +
                                    "MERGE (a:AUTHOR {id: row.id}) " +
                                    "SET a.name = row.name",
                            parameters("batch", authorBatch)
                    );

                    tx.run(
                            "UNWIND $batch AS r " +
                                    "MATCH (a:AUTHOR {id: r.aid}) " +
                                    "MATCH (p:ARTICLE {id: r.pid}) " +
                                    "MERGE (a)-[:AUTHORED]->(p)",
                            parameters("batch", authoredBatch)
                    );

                    tx.run(
                            "UNWIND $batch AS r " +
                                    "MERGE (a:ARTICLE {id: r.from}) " +
                                    "MERGE (b:ARTICLE {id: r.to}) " +
                                    "MERGE (a)-[:CITES]->(b)",
                            parameters("batch", citesBatch)
                    );

                    return null;
                });
            }

        }

        long endTime = System.currentTimeMillis();

        System.out.println("DONE");
        System.out.println("TOTAL NODES = " + count);
        System.out.println("TIME (s) = " + (endTime - startTime) / 1000);

        driver.close();
    }
}