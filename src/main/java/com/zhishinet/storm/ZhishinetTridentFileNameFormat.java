package com.zhishinet.storm;

import org.apache.storm.hdfs.trident.format.FileNameFormat;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class ZhishinetTridentFileNameFormat implements FileNameFormat {

    private int partitionIndex;
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public ZhishinetTridentFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public ZhishinetTridentFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public ZhishinetTridentFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, int partitionIndex, int numPartitions) {
        this.partitionIndex = partitionIndex;

    }

    @Override
    public String getName(long rotation, long timeStamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.HOUR_OF_DAY, 8);
        return this.prefix + sdf.format(cal.getTime()) + "/" + this.partitionIndex +  "-" + rotation + "-" + timeStamp + this.extension;
    }

    @Override
    public String getPath(){
        return this.path;
    }
}
