// Max Blankestijn & Rintse van de Vlasakker
// Storm topology for windowed aggregations

package aggregation;
import aggregation.SumAggregator;
import aggregation.MongoInsertBolt;
import aggregation.FixedSocketSpout;

import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.sql.runtime.serde.json.JsonScheme;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.streams.Pair;
import org.apache.storm.generated.*;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;

import java.util.Arrays;

public class AggregateSum {
    private static int threadsPerMachine = 32;
    private static float windowSize = 8.0f;
    private static float windowSlide = 4.0f;
    
    public static void main(String[] args) {
        // Parse arguments
        if(args.length < 4) { System.out.println("Must supply input_ip, input_port, mongo_ip, and num_workers"); }
        String input_IP = args[0];
        String input_port = args[1];
        String mongo_IP = args[2];
        Integer num_workers = Integer.parseInt(args[3]);
        Integer gen_rate = Integer.parseInt(args[4]);
        String NTP_IP = "";
        if(args.length > 5) { NTP_IP = args[5]; }
        
        // Socket spout to get input tuples
        JsonScheme inputScheme = new JsonScheme(Arrays.asList("gem", "price", "event_time"));
        FixedSocketSpout sSpout = new FixedSocketSpout(inputScheme, input_IP, Integer.parseInt(input_port));

        // Mongo bolt to store the results
        String mongo_addr = "mongodb://storm:test@" + mongo_IP + ":27017/results?authSource=admin";
        SimpleMongoMapper mongoMapper = new SimpleMongoMapper().withFields("GemID", "aggregate", "latency", "time");
        MongoInsertBolt mongoBolt = new MongoInsertBolt(mongo_addr, "aggregation", mongoMapper);

        // Build the topology (stream api)
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(sSpout, num_workers * threadsPerMachine) // Get tuples from TCP socket
            // Window the input into (8s, 4s) windows
            .window(SlidingWindows.of( // Window times in millis
                Duration.of(Math.round(1000 * windowSize / num_workers)), 
                Duration.of(Math.round(1000 * windowSlide / num_workers))
            ))
            // Map to key-value pair with the GemID as key, and an AggregationResult as value
            .mapToPair(x -> Pair.of(x.getIntegerByField("gem"), new AggregationResult(x)))
            // Aggregate the window by key
            .aggregateByKey(new SumAggregator())
            // Insert the results into the mongo database
            .to(mongoBolt);

        Config config = new Config();
        config.setNumWorkers(num_workers); // Number of supervisors to work on topology
        config.setMaxSpoutPending(Math.round(4 * windowSize * gen_rate)); // Maximum # unacked tuples

        try { StormSubmitter.submitTopologyWithProgressBar("agsum", config, builder.build()); }
        catch(AlreadyAliveException e) { System.out.println("Already alive"); }
        catch(InvalidTopologyException e) { System.out.println("Invalid topolgy"); }
        catch(AuthorizationException e) { System.out.println("Auth problem"); }
    }
}
