// Max Blankestijn & Rintse van de Vlasakker
// Storage class that represents an (intermediate) aggregation result.

package aggregation;

import org.apache.storm.tuple.Tuple;
import java.io.Serializable;
import java.time.Instant;
 

public class AgResult implements Serializable {
// Gets time from system clock
public static Double curtime() {
    Instant time = Instant.now(); // Instant.now() supports nanos since epoch
    return  Double.valueOf(time.getEpochSecond()) + 
            Double.valueOf(time.getNano()) / (1000.0*1000*1000);
}
    public Long votes; // The vote aggregate (so far)
    public Double time; // The timestamp (so far)
    public String party; // The party to aggregate for

    public AgResult(Long _votes, Double _time, String _party) {
        votes = _votes;
        time = _time;
        party = _party;
    }

    public AgResult(Tuple x) { 
        this(1L, x.getDoubleByField("event_time"), x.getStringByField("party"));
        //this(1L, curtime(), x.getStringByField("party"));
    }
    
    
    public String print() {
        return "(votes: " + Long.toString(votes) + ", party: " + party + ")";
    }
}
