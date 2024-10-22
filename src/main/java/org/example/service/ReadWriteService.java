package org.example.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.data.Metric;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReadWriteService {
    private final String jdbcURL = "jdbc:mysql://localhost:3306/mydb";
    private final String username = "user";
    private final String password = "password";
    public void writeJsonToTable(Map<String, ObjectNode> metricsMap){


        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {
            String sql = "INSERT INTO metricsTable (app_id, country_code, impressions, clicks, revenue) " +
                    "VALUES (?, ?, ?, ?, ?)";
            PreparedStatement insertStmt = connection.prepareStatement(sql);

            for (ObjectNode node : metricsMap.values()) {
                insertStmt.setInt(1, node.get("app_id").asInt());
                insertStmt.setString(2, node.get("country_code").asText());
                insertStmt.setInt(3, node.get("impressions").asInt());
                insertStmt.setInt(4, node.get("clicks").asInt());
                insertStmt.setDouble(5, node.get("revenue").asDouble());
                insertStmt.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public List<Metric> readDataFromMetrics(){
        String query = "SELECT app_id, country_code,impressions,clicks , revenue FROM metricsTable";
        List<Metric> metrics = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                Metric metric = new Metric();
                metric.setAppId(rs.getInt("app_id"));
                metric.setCountryCode(rs.getString("country_code"));
                metric.setClicks(rs.getInt("clicks"));
                metric.setImpressions(rs.getInt("impressions"));
                metric.setRevenue(rs.getDouble("revenue"));
                metrics.add(metric);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return metrics;
    }
}
