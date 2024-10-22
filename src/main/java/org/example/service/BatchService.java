package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.example.data.Metric;
import scala.Int;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BatchService {
    private final ReadWriteService readWriteService = new ReadWriteService();
    public  void process(String impressionsPathFile , String clicksPathFile ,String outputPath){
        try {
            JsonNode impressions  = reader(impressionsPathFile);
            // Map to store aggregated data
            Map<String, ObjectNode> metricsMap =  processImpression(impressions);
            JsonNode clicks = reader(clicksPathFile);
            metricsMap = processClicks(clicks,impressions,metricsMap);
            writeToJson(outputPath , metricsMap.values());
            readWriteService.writeJsonToTable(metricsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    public JsonNode reader (String pathOfFile) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(new File(pathOfFile));
        return jsonNode;
    }
    public Map<String, ObjectNode> processImpression (JsonNode impressions){
        Map<String, ObjectNode> metricsMap = new HashMap<>();
        StreamSupport.stream(impressions.spliterator(), false)
                .forEach(impression -> {
                    String appId = impression.get("app_id").asText();
                    String countryCode = impression.get("country_code").asText();
                    String key = appId + "_" + countryCode;

                    metricsMap.computeIfAbsent(key, k -> createMetricsNode(appId, countryCode))
                            .put("impressions", metricsMap.get(key).get("impressions").asInt() + 1);
                });

        return metricsMap;
    }
    public Map<String, ObjectNode> processClicks(JsonNode clicks ,JsonNode impressions  ,Map<String, ObjectNode> metricsMap){
        StreamSupport.stream(clicks.spliterator(), false)
                .forEach(click -> {
                    String impressionId = click.get("impression_id").asText();
                    double revenue = click.get("revenue").asDouble();

                    StreamSupport.stream(impressions.spliterator(), false)
                            .filter(impression -> impression.get("id").asText().equals(impressionId))
                            .findFirst()
                            .ifPresent(impression -> {
                                String appId = impression.get("app_id").asText();
                                String countryCode = impression.get("country_code").asText();
                                String key = appId + "_" + countryCode;

                                metricsMap.computeIfPresent(key, (k, metrics) -> {
                                    metrics.put("clicks", metrics.get("clicks").asInt() + 1);
                                    metrics.put("revenue", metrics.get("revenue").asDouble() + revenue);
                                    return metrics;
                                });
                            });
                });

        return metricsMap;
    }
    public void  processRecommendation(String impressionsPath , String clickPath , String outputRecommendationPath){
        SparkSession spark = SparkSession.builder()
                .appName("Top Advertisers Recommender")
                .config("spark.master", "local")
                .getOrCreate();

        // Load impressions and clicks data
        Dataset<Row> impressions = spark.read().json(impressionsPath);
        Dataset<Row> clicks = spark.read().json(clickPath);

        // Join impressions with clicks on impression_id
        Dataset<Row> joinedData = impressions.join(clicks, impressions.col("id").equalTo(clicks.col("impression_id")), "left")
                .groupBy("app_id", "country_code", "advertiser_id")
                .agg(
                        functions.count(impressions.col("id")).as("impressions"),
                        functions.sum(functions.coalesce(clicks.col("revenue"), functions.lit(0.0))).as("total_revenue")
                );

        // Calculate revenue per impression
        Dataset<Row> revenuePerImpression = joinedData.withColumn("revenue_per_impression",
                joinedData.col("total_revenue").divide(joinedData.col("impressions")));

        // Window specification for ranking advertisers
        WindowSpec windowSpec = Window.partitionBy("app_id", "country_code")
                .orderBy(revenuePerImpression.col("revenue_per_impression").desc());

        // Rank advertisers and filter top 5
        Dataset<Row> rankedData = revenuePerImpression.withColumn("rank", functions.row_number().over(windowSpec))
                .filter("rank <= 5")
                .groupBy("app_id", "country_code")
                .agg(functions.collect_list("advertiser_id").as("recommended_advertiser_ids"));

        // Save the result to a JSON file
        rankedData.write().json(outputRecommendationPath);

        spark.stop();
    }

    public void processRecommendation(String outputPath){
        List<Metric> metricList = readWriteService.readDataFromMetrics();
        List<ObjectNode> recommendationList = generateRecommendations(metricList);
        writeToJson(outputPath, recommendationList);

    }
    public void  writeToJson(String outputPath , Collection<ObjectNode> metricsMap){
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(new FileWriter(outputPath), metricsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Metrics calculated and written to :" + outputPath) ;
    }


    private static List<ObjectNode> generateRecommendations(List<Metric> metrics) {
        // Group by app_id and country_code
        Map<String, List<Metric>> groupedMetrics = metrics.stream()
                .collect(Collectors.groupingBy(m -> m.getAppId() + "_" + m.getCountryCode()!=null ? m.getCountryCode() : "null"));

        List<ObjectNode> recommendationList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        Map<Integer, Double> revenuePerImpression = new HashMap<>();
        for (Map.Entry<String, List<Metric>> entry : groupedMetrics.entrySet()) {
            String[] parts = entry.getKey().split("_");
            String appId = parts[0];

            String countryCode=parts[1];

            // Aggregate revenues and impressions by app_id within each group
            Map<Integer, Double> revenuePerImpressionMap = new HashMap<>();
            Map<Integer, Integer> impressionsMap = new HashMap<>();

            for (Metric metric : entry.getValue()) {
                revenuePerImpressionMap.merge(metric.getAppId(), metric.getRevenue(), Double::sum);
                impressionsMap.merge(metric.getAppId(), metric.getImpressions(), Integer::sum);
            }

            // Calculate revenue per impression for each advertiser

            for (Integer advertiserId : revenuePerImpressionMap.keySet()) {
                double revenue = revenuePerImpressionMap.get(advertiserId);
                int impressions = impressionsMap.get(advertiserId);
                if (impressions > 0) {
                    revenuePerImpression.put(advertiserId, revenue / impressions);
                }
            }
            // Find top 5 advertisers
            List<Integer> topAdvertisers = revenuePerImpression.entrySet().stream()
                    .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                    .limit(5)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            // Create JSON object
            ObjectNode recommendation = mapper.createObjectNode();
            recommendation.put("app_id", appId);
            recommendation.put("country_code", countryCode);
            ArrayNode advertiserIdsArray = mapper.valueToTree(topAdvertisers);
            recommendation.set("recommended_advertiser_ids", advertiserIdsArray);

            recommendationList.add(recommendation);
        }


        return recommendationList;
    }

    private static ObjectNode createMetricsNode(String appId, String countryCode) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("app_id", Integer.parseInt(appId));
        node.put("country_code", countryCode);
        node.put("impressions", 0);
        node.put("clicks", 0);
        node.put("revenue", 0.0);
        return node;
    }
}
