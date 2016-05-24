package com.hello.suripu.firehose.workers.pill;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hello.suripu.firehose.framework.WorkerConfiguration;
import io.dropwizard.db.DataSourceFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Created by ksg on 5/23/16
 */
public class PillFirehoseConfiguration extends WorkerConfiguration {

    @Valid
    @NotNull
    @JsonProperty("common_db")
    private DataSourceFactory commonDB = new DataSourceFactory();
    public DataSourceFactory getCommonDB() {
        return commonDB;
    }


}
