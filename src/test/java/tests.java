import static junit.framework.Assert.*;
import org.apache.ignite.*;

import org.junit.Test;

public class tests {
    private Ignite ignite = null;
    private IgniteCompute compute = null;
    private IgniteCache<Integer, String> cache = null;
    /**
     * Test with one line of data
     */
    @Test
    public void testOne() throws Exception {
        try {
            int res = new hw2.kafka("/home/cloudera/Pictures/hw2david/src/main/java/hw2/metrics.log").run();
            assertEquals(res,100);
        } catch (AssertionError e) {

        }
    }


}
