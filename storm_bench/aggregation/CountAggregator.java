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
        Long votes = aggregate.votes + value.votes;
        Double time = Math.max(aggregate.time, value.time);
        String county = aggregate.county != "-" ? aggregate.county : value.county; 
        
        return new AgResult(votes, time, county);
    }

    @Override // merges the partial sums
    public AgResult merge(AgResult accum1, AgResult accum2) {
        Long votes = accum1.votes + accum2.votes;
        Double time = Math.max(accum1.time, accum2.time);
        String county = accum1.county != "-" ? accum1.county : accum2.county; 
        
        return new AgResult(votes, time, county);
    }

    @Override // extract result from the accumulator
    public AgResult result(AgResult accum) { 
        System.out.println("AGGresult");
        return accum; 
    }

}
