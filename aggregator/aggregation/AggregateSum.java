// Max Blankestijn & Rintse van de Vlasakker
// Storm topology for windowed aggregations

package aggregation;
import aggregation.CountAggregator;
import aggregation.FixedSocketSpout;
import aggregation.CountyBranch;

import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.sql.runtime.serde.json.JsonScheme;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.streams.Pair;
import org.apache.storm.generated.*;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.Stream;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;
import org.apache.storm.mongodb.common.mapper.MongoUpdateMapper;

import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;


public class AggregateSum {
    private static int thread_count = 32; // Threads per machine
    private static float window_size = 8.0f; // Aggregation window size
    private static MongoUpdateBolt mongoBolt;

    private static void init_mongo(String mongo_IP) {
        String mongo_addr = "mongodb://storm:test@" + mongo_IP 
            + ":27017/results?authSource=admin";
        mongoBolt = new MongoUpdateBolt(
            mongo_addr, "aggregation"
        );
    }

    public static class PartyPred implements Predicate<Tuple> {
        private String party;
        public PartyPred(String _party) { party = _party; }
        @Override public boolean test(Tuple t) {
            return t.getStringByField("party").equals(party);
        } 
    }

    public static void main(String[] args) {
        // Parse arguments
        if(args.length < 4) { return; }
        String input_IP = args[0];
        String input_port_start = args[1];
        String mongo_IP = args[2];
        Integer num_workers = Integer.parseInt(args[3]);
        Integer gen_rate = Integer.parseInt(args[4]);        

        // Mongo bolt to store the results
        init_mongo(mongo_IP);
        PartyPred partyPreds[] = { new PartyPred("R"), new PartyPred("D") };

        // Build up the topology in terms of multiple streams
        StreamBuilder builder = new StreamBuilder();
        for(int i = 0; i < num_workers; i++) {
            // Socket spout to get input tuples
            JsonScheme inputScheme = new JsonScheme(
                Arrays.asList("county", "party", "event_time")
            );
            FixedSocketSpout sSpout = new FixedSocketSpout(
                inputScheme, input_IP, Integer.parseInt(input_port_start) + i
            );
         
            // Take input from a network socket
            Stream<Tuple>[] partyStreams = builder.newStream(sSpout, thread_count)
                .branch(partyPreds); // Split the stream into Dems and Reps

            // Aggregate votes by county in the resulting split streams
            for(int j = 0; j < partyStreams.length; j++) {
                partyStreams[j]
                    // Window the input into (8s, 4s) windows
                    .window(TumblingWindows.of( // Window times in millis
                        Duration.of(Math.round(1000 * window_size))
                    ))
                    // Map to key-value pair with the county as key, 
                    // and 1 as value (aggregation should be the count)
                    .mapToPair(x -> Pair.of(
                        x.getStringByField("county"), new AgResult(x)
                    ))
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
        config.setMaxSpoutPending(
            Math.round(4 * window_size * (gen_rate/num_workers))
        ); 
        try { 
            StormSubmitter.submitTopologyWithProgressBar(
                "agsum", config, builder.build()
            ); 
        }
        catch(Exception e) { e.printStackTrace(); }
    }
}
