package zsp.ppsoboi.analysis;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.logging.Logger;

public class HttpTriggerFunctionTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testNullRequest() {
        final HttpResponseMessage correctResponse = Mockito.mock(HttpResponseMessage.class);
        final HttpResponseMessage incorrectResponse = Mockito.mock(HttpResponseMessage.class);

        final HttpResponseMessage.Builder correctResponseBuilder = Mockito.mock(HttpResponseMessage.Builder.class);
        Mockito.doReturn(correctResponseBuilder).when(correctResponseBuilder).body(Mockito.anyString());
        Mockito.doReturn(correctResponse).when(correctResponseBuilder).build();

        final HttpResponseMessage.Builder incorrectResponseBuilder = Mockito.mock(HttpResponseMessage.Builder.class);
        Mockito.doReturn(incorrectResponseBuilder).when(incorrectResponseBuilder).body(Mockito.anyString());
        Mockito.doReturn(incorrectResponse).when(incorrectResponseBuilder).build();

        final HttpRequestMessage<Optional<HttpTriggerFunction.Request>> request =
                Mockito.mock(HttpRequestMessage.class);

        Mockito.doReturn(Optional.empty()).when(request).getBody();
        Mockito.doReturn(incorrectResponseBuilder).when(request).createResponseBuilder(Mockito.any(HttpStatus.class));
        Mockito.doReturn(correctResponseBuilder).when(request).createResponseBuilder(HttpStatus.BAD_REQUEST);

        final ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.doReturn(Logger.getGlobal()).when(context).getLogger();

        final HttpResponseMessage response = new HttpTriggerFunction().run(request, context);
        Assert.assertSame(correctResponse, response);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvalidRequest() {
        final HttpResponseMessage correctResponse = Mockito.mock(HttpResponseMessage.class);
        final HttpResponseMessage incorrectResponse = Mockito.mock(HttpResponseMessage.class);

        final HttpResponseMessage.Builder correctResponseBuilder = Mockito.mock(HttpResponseMessage.Builder.class);
        Mockito.doReturn(correctResponseBuilder).when(correctResponseBuilder).body(Mockito.anyString());
        Mockito.doReturn(correctResponse).when(correctResponseBuilder).build();

        final HttpResponseMessage.Builder incorrectResponseBuilder = Mockito.mock(HttpResponseMessage.Builder.class);
        Mockito.doReturn(incorrectResponseBuilder).when(incorrectResponseBuilder).body(Mockito.anyString());
        Mockito.doReturn(incorrectResponse).when(incorrectResponseBuilder).build();

        final HttpRequestMessage<Optional<HttpTriggerFunction.Request>> request =
                Mockito.mock(HttpRequestMessage.class);

        final HttpTriggerFunction.Request requestData = new HttpTriggerFunction.Request(null, null);
        Mockito.doReturn(Optional.of(requestData)).when(request).getBody();
        Mockito.doReturn(incorrectResponseBuilder).when(request).createResponseBuilder(Mockito.any(HttpStatus.class));
        Mockito.doReturn(correctResponseBuilder).when(request).createResponseBuilder(HttpStatus.BAD_REQUEST);

        final ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.doReturn(Logger.getGlobal()).when(context).getLogger();

        final HttpResponseMessage response = new HttpTriggerFunction().run(request, context);
        Assert.assertSame(correctResponse, response);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCorrectRequest() {
        final HttpResponseMessage correctResponse = Mockito.mock(HttpResponseMessage.class);
        final HttpResponseMessage incorrectResponse = Mockito.mock(HttpResponseMessage.class);

        final HttpResponseMessage.Builder correctResponseBuilder = Mockito.mock(HttpResponseMessage.Builder.class);
        Mockito.doReturn(correctResponseBuilder).when(correctResponseBuilder).body(Mockito.anyString());
        Mockito.doReturn(correctResponse).when(correctResponseBuilder).build();

        final HttpResponseMessage.Builder incorrectResponseBuilder = Mockito.mock(HttpResponseMessage.Builder.class);
        Mockito.doReturn(incorrectResponseBuilder).when(incorrectResponseBuilder).body(Mockito.anyString());
        Mockito.doReturn(incorrectResponse).when(incorrectResponseBuilder).build();

        final HttpRequestMessage<Optional<HttpTriggerFunction.Request>> request =
                Mockito.mock(HttpRequestMessage.class);

        final HttpTriggerFunction.Request requestData = new HttpTriggerFunction.Request("name", 2);
        Mockito.doReturn(Optional.of(requestData)).when(request).getBody();
        Mockito.doReturn(incorrectResponseBuilder).when(request).createResponseBuilder(Mockito.any(HttpStatus.class));
        Mockito.doReturn(correctResponseBuilder).when(request).createResponseBuilder(HttpStatus.OK);

        final ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.doReturn(Logger.getGlobal()).when(context).getLogger();

        final HttpResponseMessage response = new HttpTriggerFunction().run(request, context);
        Assert.assertSame(correctResponse, response);
    }
}
