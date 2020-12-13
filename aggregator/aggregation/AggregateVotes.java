// Max Blankestijn & Rintse van de Vlasakker

package aggregation;
import aggregation.AggregatorBolt;
import aggregation.FixedSocketSpout;

import org.apache.storm.sql.runtime.serde.json.JsonScheme;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;
import org.apache.storm.mongodb.common.mapper.MongoUpdateMapper;

import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;


public class AggregateVotes {
    // Component configuration
    private static int BATCH_SECS = 6;
    private static int FLUSH_SECS = 4;

    // Cluster parameters
    private static String input_IP;
    private static Integer input_port_start;
    private static String mongo_IP;
    private static String mongo_lat_IP;
    private static Integer num_workers;
    private static Integer num_streams;
    private static long gen_rate;

    private static MongoUpdateBolt new_mongobolt(
        String mongo_IP, String mongo_lat_IP
    ) {
        String data_addr = "mongodb://storm:test@" + mongo_IP 
            + ":27017/results?authSource=admin";
        String lat_addr = "mongodb://storm:test@" + mongo_lat_IP 
            + ":27017/results?authSource=admin";
        return new MongoUpdateBolt(
            data_addr, "aggregation", lat_addr, "latencies", 
            StateSplitBolt.states.length, FLUSH_SECS
        );
    }

    private static FixedSocketSpout new_spout(int offset) {
        return new FixedSocketSpout(
            new JsonScheme(Arrays.asList("id", "state", "party", "event_time")), 
            input_IP, input_port_start + offset
        );
    }

    private static AggregatorBolt new_aggbolt(String state) {
        return new AggregatorBolt(
            state, BATCH_SECS*gen_rate, FLUSH_SECS
        );
    }

    public static void main(String[] args) {
        assert(BATCH_SECS < FLUSH_SECS && FLUSH_SECS < 2 * BATCH_SECS);

        // Parse arguments
        if(args.length < 5) { return; }
        input_IP = args[0];
        input_port_start = Integer.parseInt(args[1]);
        mongo_IP = args[2];
        mongo_lat_IP = args[3];
        
        num_workers = Integer.parseInt(args[4]);
        assert(num_workers > 1);

        num_streams = Integer.parseInt(args[5]);
        assert(num_streams > 1);

        gen_rate = Long.parseLong(args[6]);
        assert(gen_rate > num_workers);

        TopologyBuilder builder = new TopologyBuilder();

        for(int i = 0; i < num_streams; i++) {
            String streamId = Integer.toString(i);
            // Take input from a network socket
            builder.setSpout("socket-" + streamId, new_spout(i), 1);

            // Send each state to a differing aggregatorbolt
            builder.setBolt("split-" + streamId, new StateSplitBolt(), 1)
                .shuffleGrouping("socket-" + streamId);
            
            // Aggregate by state, one bolt for each state
            for(String state : StateSplitBolt.states) {
                builder.setBolt("agg-" + state + "-" + streamId, new_aggbolt(state), 1)
                    .shuffleGrouping("split-" + streamId, state);
            }

            // Store results to mongo
            BoltDeclarer mongobolt = builder.setBolt(
                "mongo-" + streamId, new_mongobolt(mongo_IP, mongo_lat_IP), 1
            );
            // Receive from all aggregators
            for(String state : StateSplitBolt.states) {
                mongobolt.shuffleGrouping("agg-" + state + "-" + streamId);
            }
        }

        // Config and submission
        Config config = new Config();
        config.setNumWorkers(num_workers);
        config.setMessageTimeoutSecs(3*FLUSH_SECS);
        config.setMaxSpoutPending( // Maximum # unacked tuples
            Math.round(20 * (gen_rate/num_streams))
        ); 
        try { 
            StormSubmitter.submitTopologyWithProgressBar(
                "agsum", config, builder.createTopology()
            ); 
        }
        catch(Exception e) { e.printStackTrace(); }
    }
}
