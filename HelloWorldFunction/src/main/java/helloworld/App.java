package helloworld;

import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.lambda.powertools.aurora.DataStream;
import software.amazon.lambda.powertools.aurora.DataStreamHandler;
import software.amazon.lambda.powertools.aurora.model.PostgresActivityEvent;
import software.amazon.lambda.powertools.aurora.model.PostgresActivityRecords;
import software.amazon.lambda.powertools.tracing.Tracing;
import software.amazon.lambda.powertools.metrics.Metrics;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import software.amazon.lambda.powertools.aurora.AuroraUtils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<KinesisEvent, String> {

    LambdaLogger logger;

    @Tracing(namespace = "aurora-namespace-s1")
    @Metrics(captureColdStart = true)
    @Override
    public String handleRequest(final KinesisEvent input, final Context context) {
        logger = context.getLogger();
        List values = AuroraUtils.process(input, SampleDataHandler.class);
        logger.log("Values processed " + values.toString());
        return "ok";
    }

    public class SampleDataHandler implements DataStreamHandler<Object> {

        @Override
        public PostgresActivityEvent process(PostgresActivityEvent record) {
            System.out.println("Custom process method " + record.toString());
            return record;
        }

    }
}
