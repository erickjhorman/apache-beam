import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class FilterOperationTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testFilterPCollection_FilterCountriesByGivenLetter() {

        List<String> expectedOutput = List.of("Togo", "Tanzania", "Thailand");

        PCollection<String> inputFile = pipeline.apply(Create.of(expectedOutput));
        PCollection<String> finalResult = inputFile.apply(ParDo.of(new FilterByInitialLetter("T")));

        PAssert.that(finalResult).containsInAnyOrder(expectedOutput);
        pipeline.run();
    }
}