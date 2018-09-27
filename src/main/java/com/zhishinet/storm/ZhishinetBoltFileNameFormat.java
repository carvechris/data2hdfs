package com.zhishinet.storm;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class ZhishinetBoltFileNameFormat implements FileNameFormat {

    private static final Logger logger = LoggerFactory.getLogger(ZhishinetBoltFileNameFormat.class);

    private String componentId;
    private int taskId;
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";
    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public ZhishinetBoltFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public ZhishinetBoltFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public ZhishinetBoltFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.HOUR_OF_DAY, 8);
        return this.prefix +sdf.format(cal.getTime()) + "/" + this.componentId + "-" + this.taskId +  "-" + rotation + "-" + timeStamp + this.extension;
    }

    @Override
    public String getPath(){
        return this.path;
    }
}
