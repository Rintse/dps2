package aggregation;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class StateSplitBolt extends BaseRichBolt {
    OutputCollector collector;
    public static final String states[] = {
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL",
        "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT",
        "NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
        "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
    };

    @Override
    public void prepare(
        Map<String, Object> topoConf, 
        TopologyContext context, 
        OutputCollector collector
    ) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) { return; }
        else {
            collector.emit(tuple.getStringByField("state"), tuple, tuple.getValues());
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(String state: states) {
            declarer.declareStream(
                state, new Fields("id", "state", "party", "event_time")
            );
        }
    }
}
