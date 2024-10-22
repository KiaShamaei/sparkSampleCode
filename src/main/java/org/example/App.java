package org.example;

import org.example.service.BatchService;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        BatchService service = new BatchService();
        service.process("impressions.json","clicks.json" ,"output.json" );
//        service.processRecommendation("impressions.json"
//                , "clicks.json"
//                , "recommended_advertisers.json");
        service.processRecommendation("recommendation_output.json");
    }
}
