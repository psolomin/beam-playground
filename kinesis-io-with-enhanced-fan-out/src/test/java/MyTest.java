import java.io.IOException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MyTest {
    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();

    @Test
    public void failingTest() throws IOException {
        p.run().waitUntilFinish();
    }
}
