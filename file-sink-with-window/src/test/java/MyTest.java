import com.pavel.example.MyConverters;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MyTest {
    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();

    @Test(expected=Pipeline.PipelineExecutionException.class)
    public void failingTest() throws IOException {
        File basePathF = tmpFolder.newFolder("main");
        String basePath = basePathF.toString();

        PCollection<Integer> windowed = p.apply(Create.of(List.of(1, 2, 3, 4, 5)))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));

        PCollectionView<Map<Integer, String>> schemasView = MyConverters.schemasView(windowed);
        MyConverters.applyWriter(windowed, basePath, schemasView);
        p.run().waitUntilFinish();
    }

    @Test
    public void passingTest() throws IOException {
        File basePathF = tmpFolder.newFolder("main");
        String basePath = basePathF.toString();
        PCollection<Integer> windowed = p.apply(Create.of(List.of(1, 2, 3, 4, 5)));
        PCollectionView<Map<Integer, String>> schemasView = MyConverters.schemasView(windowed);
        MyConverters.applyWriter(windowed, basePath, schemasView);
        p.run().waitUntilFinish();
    }
}
