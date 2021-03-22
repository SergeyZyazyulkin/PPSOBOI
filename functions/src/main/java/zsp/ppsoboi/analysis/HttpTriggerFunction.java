package zsp.ppsoboi.analysis;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Azure Functions with HTTP Trigger.
 */
public class HttpTriggerFunction {

    private static final String DB_NAME = "ppsoboi-data";
    private static final String COLLECTION_NAME = "data";

    private static final MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://ppsoboi:pjVGVBb8gsU0JmQWlLfx2oMkzrhnMWO37h67BwqjyI3mALvKLg9RVZUuw9UzKj8N6uvf9MK9dCzJVM6ur3VVKA==@ppsoboi.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@ppsoboi@"));


    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */
    @FunctionName("analysis")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<Request>> request,
            final ExecutionContext context) {

        final Request req = request.getBody().orElse(null);
        context.getLogger().info(String.format("Processing request %s...", req));

        if (req == null) {
            context.getLogger().info("Processed request null: - ignored");
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("null request").build();
        } else if (!req.isValid()) {
            context.getLogger().info(String.format("Processed request %s: invalid request - field is null", req));
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("invalid request").build();
        } else {
            final MongoDatabase mongoDatabase = mongoClient.getDatabase(DB_NAME);
            final MongoCollection<Document> collection = mongoDatabase.getCollection(COLLECTION_NAME);

            final Bson group = Aggregates.group("$" + req.field, Accumulators.sum("count", 1));

            final Bson project = Aggregates.project(Projections.fields(
                    Projections.excludeId(), Projections.include("count"), Projections.computed("value", "$_id")));

            final Bson sort = Aggregates.sort(Sorts.descending("count"));
            final Bson limit = Aggregates.limit(req.getLimit() != null ? req.getLimit() : 10);

            final List<Document> results =
                    collection.aggregate(Arrays.asList(group, project, sort, limit)).into(new ArrayList<>());

            final String response = results.stream().map(Document::toJson).collect(Collectors.joining(","));
            context.getLogger().info(String.format("Processed request %s: OK", req));
            return request.createResponseBuilder(HttpStatus.OK).body("[" + response + "]").build();
        }
    }

    public static final class Request {

        private String field;
        private Integer limit;

        public String getField() {
            return field;
        }

        public Integer getLimit() {
            return limit;
        }

        public boolean isValid() {
            return field != null;
        }

        @Override
        public String toString() {
            return String.format("Request(field=%s,limit=%d)", field, limit);
        }
    }
}
