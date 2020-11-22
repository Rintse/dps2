// Max Blankestijn & Rintse van de Vlasakker
// Storage class that represents an (intermediate) aggregation result.

package aggregation;

import org.apache.storm.tuple.Tuple;
import java.io.Serializable;

 
public class AgResult implements Serializable {
    public Long votes; // The vote aggregate (so far)
    public Double time; // The timestamp (so far)
    public String party; // The county to aggregate for

    public AgResult(Long _votes, Double _time, String _party) {
        votes = _votes;
        time = _time;
        party = _party;
    }

    public AgResult(Tuple x) {
        this(1L, x.getDoubleByField("event_time"), x.getStringByField("party"));
    }
    
    public String print() {
        return "(votes: " + Long.toString(votes) + ", party: " + party + ")";
    }
}
