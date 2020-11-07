// Max Blankestijn & Rintse van de Vlasakker
// Storage class that represents an (intermediate) aggregation result.

package aggregation;

import org.apache.storm.tuple.Tuple;
import java.io.Serializable;


public class AggregationResult implements Serializable {
    public Integer price; // The aggregate price (so far)
    public Double event_time; // The highest event_time (so far)

    public AggregationResult(Integer _price, Double _event_time) {
        price = _price;
        event_time = _event_time;
    }

    public AggregationResult(Tuple x) {
        this(x.getIntegerByField("price"), x.getDoubleByField("event_time"));
    }
}
