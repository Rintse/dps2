// Max Blankestijn & Rintse van de Vlasakker
// Storm topology for windowed aggregations

package aggregation;
import aggregation.CountAggregator;
import aggregation.FixedSocketSpout;

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


public class AggregateVotes {
    private static int core_count = 16; // Threads per machine
    private static float window_size = 2.0f; // Aggregation window size
    private static MongoUpdateBolt mongoBolt;

    private static void init_mongo(
        String mongo_IP, String mongo_lat_IP
    ) {
        String data_addr = "mongodb://storm:test@" + mongo_IP 
            + ":27017/results?authSource=admin";
        String lat_addr = "mongodb://storm:test@" + mongo_lat_IP 
            + ":27017/results?authSource=admin";
        mongoBolt = new MongoUpdateBolt(
            data_addr, "aggregation", lat_addr, "latencies", 
            Math.round(window_size)
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
        if(args.length < 5) { return; }
        String input_IP = args[0];
        Integer input_port_start = Integer.parseInt(args[1]);
        String mongo_IP = args[2];
        String mongo_lat_IP = args[3];
        
        Integer num_workers = Integer.parseInt(args[4]);
        assert(num_workers > 1);

        Integer num_streams = Integer.parseInt(args[5]);
        assert(num_streams > 1);

        Integer gen_rate = Integer.parseInt(args[6]);
        assert(gen_rate > num_workers);

        // Mongo bolt to store the results
        init_mongo(mongo_IP, mongo_lat_IP);
        PartyPred partyPreds[] = { new PartyPred("R"), new PartyPred("D") };

        // Build up the topology in terms of multiple streams
        StreamBuilder builder = new StreamBuilder();
        for(int i = 0; i < num_streams; i++) {
            // Socket spout to get input tuples
            FixedSocketSpout sSpout = new FixedSocketSpout(
                new JsonScheme(Arrays.asList("state", "party", "event_time")), 
                input_IP, input_port_start + i
            );

            // Take input from a network socket
            Stream<Tuple>[] partyStreams = builder.newStream(sSpout, core_count)
                .branch(partyPreds); // Split the stream into Dems and Reps

            // Aggregate votes by state in the resulting split streams
            for(int j = 0; j < partyStreams.length; j++) {
                partyStreams[j]
                    // Window the input
                    .window(TumblingWindows.of( // Window times in millis
                        Duration.of(Math.round(1000 * window_size))
                    ))
                    // Map to key-value pair with the state as key
                    .mapToPair(x -> Pair.of(
                        x.getStringByField("state"), new AgResult(x)
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
        config.setMessageTimeoutSecs(Math.round(3*window_size));
        // Maximum # unacked tuples
        config.setMaxSpoutPending(
            Math.round(50 * window_size * (gen_rate/num_streams))
        ); 
        try { 
            StormSubmitter.submitTopologyWithProgressBar(
                "agsum", config, builder.build()
            ); 
        }
        catch(Exception e) { e.printStackTrace(); }
    }
}
