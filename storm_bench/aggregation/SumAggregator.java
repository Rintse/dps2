// Max Blankestijn & Rintse van de Vlasakker
// Functor that defines how aggregation should be performed

package aggregation;
import aggregation.AggregationResult;

import org.apache.storm.streams.operations.CombinerAggregator;

// Aggregates sum, while finding the minimum event time.
public class SumAggregator implements CombinerAggregator<AggregationResult, AggregationResult, AggregationResult> {

    @Override // The initial value of the sum
    public AggregationResult init() { return new AggregationResult(0, Double.NEGATIVE_INFINITY); }

    @Override // Updates the sum by adding the value (this could be a partial sum)
    public AggregationResult apply(AggregationResult aggregate, AggregationResult value) {
        return new AggregationResult(
            aggregate.price + value.price,
            Math.max(aggregate.event_time, value.event_time)
        );
    }

    @Override // merges the partial sums
    public AggregationResult merge(AggregationResult accum1, AggregationResult accum2) {
        return new AggregationResult(
            accum1.price + accum2.price,
            Math.max(accum1.event_time, accum2.event_time)
        );
    }

    @Override // extract result from the accumulator
    public AggregationResult result(AggregationResult accum) { System.out.println("AGGresult"); return accum; }
}
