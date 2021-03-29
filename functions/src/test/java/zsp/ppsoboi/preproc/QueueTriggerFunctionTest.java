package zsp.ppsoboi.preproc;

import com.microsoft.azure.functions.ExecutionContext;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.logging.Logger;

public class QueueTriggerFunctionTest {

    @Test
    public void testNullRequest() {
        final Logger logger = Mockito.mock(Logger.class);
        final String expectedMessage = "Failed to process job: null";

        final ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.doReturn(logger).when(context).getLogger();

        new QueueTriggerFunction().run(null, context);
        Mockito.verify(logger).info(expectedMessage);
    }

    @Test
    public void testCorrectRequest() {
        final Logger logger = Mockito.mock(Logger.class);
        final String expectedMessage = "Processed job 'test'";

        final ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.doReturn(logger).when(context).getLogger();

        final QueueTriggerFunction.RawJobView job =
                new QueueTriggerFunction.RawJobView("test", "1", "test", "test", "test");

        new QueueTriggerFunction().run(job, context);
        Mockito.verify(logger).info(expectedMessage);
    }
}
