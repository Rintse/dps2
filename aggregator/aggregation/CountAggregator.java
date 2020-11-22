// Max Blankestijn & Rintse van de Vlasakker
// Functor that defines how aggregation should be performed

package aggregation;
import aggregation.AgResult;

import org.apache.storm.streams.operations.CombinerAggregator;

// Aggregates sum, while finding the minimum event time.
public class CountAggregator 
implements CombinerAggregator<AgResult, AgResult, AgResult> {
    
    @Override // The initial value of the sum
    public AgResult init() { 
        return new AgResult(0L, Double.NEGATIVE_INFINITY, "-");
    }

    @Override // Updates the sum by adding the value (this could be a partial sum)
    public AgResult apply(AgResult aggregate, AgResult value) {
        aggregate.votes += value.votes;
        aggregate.time = Math.max(aggregate.time, value.time);
        aggregate.party = aggregate.party != "-" ? aggregate.party : value.party; 

        return aggregate;
    }

    @Override // merges the partial sums
    public AgResult merge(AgResult accum1, AgResult accum2) {
        accum1.votes += accum2.votes;
        accum1.time = Math.max(accum1.time, accum2.time);
        accum1.party = accum1.party != "-" ? accum1.party : accum2.party; 
       
        return accum1;
    }

    @Override // extract result from the accumulator
    public AgResult result(AgResult accum) { 
        return accum; 
    }

}
