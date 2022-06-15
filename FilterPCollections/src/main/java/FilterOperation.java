import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class FilterOperation {

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
        pipeline.apply("Read files", TextIO.read().from(myOptions.getInput()))
                .apply("Split countries from file", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line ) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply("Filter Empty Word", Filter.by((String word) -> !word.isEmpty()))
                .apply("Filter by a target letter", ParDo.of(new FilterByInitialLetter(myOptions.getLetter())))
                .apply(Log.ofElements())
                .apply("Writing in a new file" , TextIO.write()
                        .to(myOptions.getOutput())
                        .withSuffix(".txt")
                        .withoutSharding());

        pipeline.run();


    }
        public interface MyOptions extends PipelineOptions {

           @Description("Input file to be read")
           @Validation.Required
           String getInput();

            void setInput(String input);

            @Description("Output file to save")
            @Default.String("output/output")
            String getOutput();

            void setOutput(String output);

            @Description("Letter to filter")
            @Validation.Required
            String getLetter();

            void setLetter(String letter);

        }
}
