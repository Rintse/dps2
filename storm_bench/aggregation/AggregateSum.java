// Max Blankestijn & Rintse van de Vlasakker
// Storm topology for windowed aggregations

package aggregation;
import aggregation.CountAggregator;
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
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.streams.Stream;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.io.IOException;


public class AggregateSum {
    private static int thread_count = 32; // Threads per machine
    private static float window_size = 8.0f; // Aggregation window size
    private static float window_slide = 4.0f; // Aggregation window slide

    public static class countyPredicate implements Predicate<Tuple> {
        String testCounty;

        public countyPredicate(String county) {
            testCounty = county;
        }
        @Override
        public boolean test(Tuple t) {
            return t.getStringByField("county") == testCounty;
        } 
    }

    public static List<String> readCountyList(String fileName) throws IOException {
        List<String> result;
        try (java.util.stream.Stream<String> lines = Files.lines(Paths.get(fileName))) {
            result = lines.collect(Collectors.toList());
        } 
        return result;
    }

    public static void main(String[] args) {
        // Parse arguments
        if(args.length < 4) { return; }
        String input_IP = args[0];
        String input_port_start = args[1];
        String mongo_IP = args[2];
        Integer num_workers = Integer.parseInt(args[3]);
        Integer gen_rate = Integer.parseInt(args[4]);        

        List<String> counties;
        try { 
            counties = readCountyList("../counties.dat");
        } catch(IOException e) {
            System.out.println("Could not read county file."); 
            return;
        }
        countyPredicate[] predicates = new countyPredicate[counties.size()];

        for(int i = 0; i < counties.size(); i++) {
            predicates[i] = (new countyPredicate(counties.get(i)));
        }

        // Mongo bolt to store the results
        String mongo_addr   = "mongodb://storm:test@" + mongo_IP 
                            + ":27017/results?authSource=admin";
        
        SimpleMongoMapper mongoMapper = 
            new SimpleMongoMapper().withFields("county", "votecount");
        
        MongoInsertBolt mongoBolt = new MongoInsertBolt(
            mongo_addr, "aggregation", mongoMapper
        );

        // Build up the topology in terms of multiple streams
        StreamBuilder builder = new StreamBuilder();
        for(int i = 0; i < num_workers; i++) {
            // Socket spout to get input tuples
            JsonScheme inputScheme = new JsonScheme(
                Arrays.asList("county", "party", "time")
            );
            FixedSocketSpout sSpout = new FixedSocketSpout(
                inputScheme, input_IP, Integer.parseInt(input_port_start) + i
            );
         
            // Stream that processes tuples from the spout
            Stream<Tuple>[] partyStreams = builder.newStream(sSpout, thread_count)
                // Window the input into (8s, 4s) windows
                .window(SlidingWindows.of( // Window times in millis
                    Duration.of(Math.round(1000 * window_size)), 
                    Duration.of(Math.round(1000 * window_slide))
                ))
                // Split the stream into a stream for Dems and Reps
                .branch(predicates);

            for(int j = 0; j < 2; j++) {
                partyStreams[j]
                    // Map to key-value pair with the county as key, 
                    // and 1 as value (aggregation should be the count)
                    .mapToPair(x -> Pair.of(x.getStringByField("party"), 1))
                    // Aggregate the window by key
                    .aggregateByKey(new CountAggregator())
                    // Insert the results into the mongo database
                    .to(mongoBolt);
            }
        }

        // Config and submission
        Config config = new Config();
        config.setNumWorkers(num_workers); 
        // Maximum # unacked tuples
        config.setMaxSpoutPending(Math.round(4 * window_size * gen_rate)); 
        try { 
            StormSubmitter.submitTopologyWithProgressBar(
                "agsum", config, builder.build()
            ); 
        }
        catch(Exception e) { System.out.println("Something went wrong"); }
    }
}
