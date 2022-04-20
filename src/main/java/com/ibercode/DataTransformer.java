package com.ibercode;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsInputPreprocessingResponse;
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DataTransformer implements RequestHandler<KinesisFirehoseEvent, KinesisAnalyticsInputPreprocessingResponse> {

    @Override
    public KinesisAnalyticsInputPreprocessingResponse handleRequest(KinesisFirehoseEvent event, Context context) {

        KinesisAnalyticsInputPreprocessingResponse response = new KinesisAnalyticsInputPreprocessingResponse();
        List<KinesisAnalyticsInputPreprocessingResponse.Record> records = new ArrayList<>();

        event.getRecords()
                .stream()
                .forEach(r -> {
                            byte[] content = r.getData().array();
                            String rawContent = new String(content, StandardCharsets.UTF_8);

                            //Data transformation
                            String extract = rawContent.split("\"SourceDDBTable\",")[1].split(",\"eventSource\"")[0];

                            ByteBuffer data = ByteBuffer.wrap(extract.getBytes(StandardCharsets.UTF_8));
                            KinesisAnalyticsInputPreprocessingResponse.Result result = KinesisAnalyticsInputPreprocessingResponse.Result.Ok;
                            KinesisAnalyticsInputPreprocessingResponse.Record record =
                                    new KinesisAnalyticsInputPreprocessingResponse.Record(r.getRecordId(), result, data);

                            records.add(record);
                        }
                );

        response.setRecords(records);
        return response;
    }

}
