import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;

@RequiredArgsConstructor
public class FilterByInitialLetter extends DoFn<String, String> {

    private final String letter;

    @ProcessElement
    public void process(@Element String elem, ProcessContext c) {
        if(elem.startsWith(letter)){
            c.output(elem);
        }

    }
}
