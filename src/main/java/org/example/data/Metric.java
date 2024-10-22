package org.example.data;

public class Metric {
    private int appId;
    private String countryCode;
    private int impressions;

    private int clicks;
    private double revenue;
    public int getClicks() {
        return clicks;
    }

    public void setClicks(int clicks) {
        this.clicks = clicks;
    }

    // Getters and setters
    public int getAppId() { return appId; }
    public void setAppId(int appId) { this.appId = appId; }

    public String getCountryCode() { return countryCode; }
    public void setCountryCode(String countryCode) { this.countryCode = countryCode; }

    public int getImpressions() { return impressions; }
    public void setImpressions(int impressions) { this.impressions = impressions; }

    public double getRevenue() { return revenue; }
    public void setRevenue(double revenue) { this.revenue = revenue; }
}
