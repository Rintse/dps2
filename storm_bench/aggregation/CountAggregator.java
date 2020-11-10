// Max Blankestijn & Rintse van de Vlasakker
// Functor that defines how aggregation should be performed

package aggregation;

import org.apache.storm.streams.operations.CombinerAggregator;

// Aggregates sum, while finding the minimum event time.
public class CountAggregator implements CombinerAggregator<Integer, Long, Long> {

    @Override // The initial value of the sum
    public Long init() { 
        return 0L;
    }

    @Override // Updates the sum by adding the value (this could be a partial sum)
    public Long apply(Long aggregate, Integer value) {
        return aggregate + value;
    }

    @Override // merges the partial sums
    public Long merge(Long accum1, Long accum2) {
        return accum1 + accum2;
    }

    @Override // extract result from the accumulator
    public Long result(Long accum) { 
        System.out.println("AGGresult");
        return accum; 
    }
}
