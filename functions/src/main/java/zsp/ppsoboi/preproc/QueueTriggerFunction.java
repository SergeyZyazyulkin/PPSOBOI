package zsp.ppsoboi.preproc;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.ServiceBusQueueTrigger;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Azure Functions with Azure Storage Queue trigger.
 */
public class QueueTriggerFunction {

    private static final Pattern SALARY_PATTERN = Pattern.compile("([0-9]+)(-)?([0-9]+)?(.*)");
    private static final String DB_NAME = "ppsoboi-data";
    private static final String COLLECTION_NAME = "data";

    private static final MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://ppsoboi:pjVGVBb8gsU0JmQWlLfx2oMkzrhnMWO37h67BwqjyI3mALvKLg9RVZUuw9UzKj8N6uvf9MK9dCzJVM6ur3VVKA==@ppsoboi.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@ppsoboi@"));

    /**
     * This function will be invoked when a new message is received at the specified path. The message contents are provided as input to this function.
     */
    @FunctionName("data-preprocessing")
    public void run(
            @ServiceBusQueueTrigger(
                    name = "message", queueName = "data-queue", connection = "MyStorageConnectionAppSetting")
                    RawJobView message,
            final ExecutionContext context
    ) {
        try {
            context.getLogger().info(String.format("Processing job '%s'...", message.getName()));

            final ProcessedJobView processedJob = new ProcessedJobView();
            processedJob.setName(parseName(message.getName()));
            processedJob.setMinSalary(parseMinSalary(message.getSalary()));
            processedJob.setMaxSalary(parseMaxSalary(message.getSalary()));
            processedJob.setPlace(parsePlace(message.getPlace()));
            processedJob.setEmployer(parseEmployer(message.getEmployer()));
            processedJob.setDescription(parseDescription(message.getDescription()));

            final MongoDatabase mongoDatabase = mongoClient.getDatabase(DB_NAME);
            final MongoCollection<Document> collection = mongoDatabase.getCollection(COLLECTION_NAME);
            collection.insertOne(processedJob.toDocument());

            context.getLogger().info(String.format("Processed job '%s'", message.getName()));
        } catch (final Exception e) {
            context.getLogger().info("Failed to process job: " + e.getMessage());
        }
    }

    private static Integer parseMinSalary(final String salary) {
        return parseSalary(salary, true);
    }

    private static Integer parseMaxSalary(final String salary) {
        return parseSalary(salary, false);
    }

    private static Integer parseSalary(final String salary, final boolean min) {
        if (salary != null) {
            final Matcher m = SALARY_PATTERN.matcher(salary);

            if (m.find()) {
                final int minSalary = Integer.parseInt(m.group(1));
                if (min) return minSalary;
                final String maxSalary = m.group(3);
                return maxSalary != null ? Integer.parseInt(maxSalary) : minSalary;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private static String parseName(final String name) {
        return parseStringValue(name);
    }

    private static String parsePlace(final String place) {
        return parseStringValue(place);
    }

    private static String parseEmployer(final String employer) {
        return parseStringValue(employer);
    }

    private static String parseDescription(final String description) {
        return parseStringValue(description);
    }

    private static String parseStringValue(final String str) {
        return str == null || str.trim().isEmpty()
                ? null
                : str.trim();
    }

    public static final class RawJobView {

        private String name;
        private String salary;
        private String place;
        private String employer;
        private String description;

        public RawJobView() {
        }

        public RawJobView(final String name, final String salary, final String place, final String employer, final String description) {
            this.name = name;
            this.salary = salary;
            this.place = place;
            this.employer = employer;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getSalary() {
            return salary;
        }

        public String getPlace() {
            return place;
        }

        public String getEmployer() {
            return employer;
        }

        public String getDescription() {
            return description;
        }
    }

    public static final class ProcessedJobView {

        private String name;
        private Integer minSalary;
        private Integer maxSalary;
        private String place;
        private String employer;
        private String description;

        public void setName(final String name) {
            this.name = name;
        }

        public void setMinSalary(final Integer minSalary) {
            this.minSalary = minSalary;
        }

        public void setMaxSalary(final Integer maxSalary) {
            this.maxSalary = maxSalary;
        }

        public void setPlace(final String place) {
            this.place = place;
        }

        public void setEmployer(final String employer) {
            this.employer = employer;
        }

        public void setDescription(final String description) {
            this.description = description;
        }

        public Document toDocument() {
            final Document document = new Document();
            document.append("name", name);
            document.append("minSalary", minSalary);
            document.append("maxSalary", maxSalary);
            document.append("place", place);
            document.append("employer", employer);
            document.append("description", description);
            return document;
        }
    }
}
