// Max Blankestijn & Rintse van de Vlasakker
// Storm topology for windowed aggregations

package aggregation;
import aggregation.AggregatorBolt;
import aggregation.FixedSocketSpout;

import org.apache.storm.sql.runtime.serde.json.JsonScheme;
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
    private static int NUM_STATES = 50;
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

        TopologyBuilder builder = new TopologyBuilder();

        for(int i = 0; i < num_streams; i++) {
            String idstr = Integer.toString(i);
            // Take input from a network socket
            FixedSocketSpout sSpout = new FixedSocketSpout(
                new JsonScheme(Arrays.asList("id", "state", "party", "event_time")), 
                input_IP, input_port_start + i
            );
            builder.setSpout("socket-" + idstr, sSpout, 1);

            // Aggregate by state
            builder.setBolt("agg-" + idstr, new AggregatorBolt(5000L), core_count)
                .setNumTasks(NUM_STATES)
                .fieldsGrouping("socket-" + idstr, new Fields("state"));

            // Store results to mongo
            builder.setBolt("mongo-" + idstr, mongoBolt, 1)
                .shuffleGrouping("agg-" + idstr);
        }

        // Config and submission
        Config config = new Config();
        config.setNumWorkers(num_workers);
        config.setMessageTimeoutSecs(Math.round(4*window_size));
        // Maximum # unacked tuples
        config.setMaxSpoutPending(
            Math.round(50 * window_size * (gen_rate/num_streams))
        ); 
        try { 
            StormSubmitter.submitTopologyWithProgressBar(
                "agsum", config, builder.createTopology()
            ); 
        }
        catch(Exception e) { e.printStackTrace(); }
    }
}
