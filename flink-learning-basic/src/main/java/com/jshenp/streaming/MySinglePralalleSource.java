package com.jshenp.streaming;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySinglePralalleSource implements SourceFunction<Long> {

    private Long number = 1L;
    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            number++;
            sourceContext.collect(number);
            number++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
