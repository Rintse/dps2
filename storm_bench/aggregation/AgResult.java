// Max Blankestijn & Rintse van de Vlasakker
// Storage class that represents an (intermediate) aggregation result.

package aggregation;

import org.apache.storm.tuple.Tuple;
import java.io.Serializable;

 
public class AgResult implements Serializable {
    public Long votes; // The vote aggregate (so far)
    public Double time; // The timestamp (so far)
    public String county; // The county to aggregate for

    public AgResult(Long _votes, Double _time, String _county) {
        votes = _votes;
        time = _time;
        county = _county;
    }

    public AgResult(Tuple x, Long l) {
        this(l, x.getDoubleByField("time"), x.getStringByField("county"));
    }
}
