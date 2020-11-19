package aggregation;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.streams.operations.Predicate;

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.io.IOException;


public class CountyBranch {
    private List<String> counties;

    public static class countyPredicate implements Predicate<Tuple> {
        String testCounty;

        public countyPredicate(String county) {
            testCounty = county;
        }
        @Override
        public boolean test(Tuple t) {
            return t.getStringByField("county") == testCounty;
        } 
    }

    public countyPredicate[] gen_preds() throws IOException {
        readCountyList("../counties.dat");
        countyPredicate[] county_preds = new countyPredicate[counties.size()];
        
        for(int i = 0; i < counties.size(); i++) {
            county_preds[i] = (new countyPredicate(counties.get(i)));
        }
        
        return county_preds;
    }

    private void readCountyList(String fileName) throws IOException {
        try (java.util.stream.Stream<String> lines = 
            Files.lines(Paths.get(fileName))
        ) {
            counties = lines.collect(Collectors.toList());
        } 
    }
}
