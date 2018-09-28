package com.zhishinet.burypoint;

import org.apache.storm.tuple.Fields;

public class Field {

    public static final String USERID = "userid";
    public static final String OPENID = "openid";
    public static final String USERNAME = "username";
    public static final String FULLNAME = "fullname";
    public static final String USERTYPE = "userType";
    public static final String OSNAME = "osName";
    public static final String OSVERSION = "osVersion";
    public static final String IP = "ip";
    public static final String PLATFORM = "platform";
    public static final String APPNAME = "appName";
    public static final String ACTION = "action";
    public static final String ACTIONTIME = "actionTime";
    public static final String NETTYPE = "netType";
    public static final String LANGUAGE = "language";
    public static final String SUCCESS = "success";
    public static final String ERRORCODE = "errorCode";
    public static final String CREATEDON = "createdOn";

    public static final Fields kafkaMessageFields = new Fields(
            Field.USERID,Field.OPENID,Field.USERNAME,Field.FULLNAME,
            Field.USERTYPE,Field.OSNAME,Field.OSVERSION,Field.IP,
            Field.PLATFORM, Field.APPNAME,Field.ACTION,Field.ACTIONTIME,
            Field.NETTYPE,Field.LANGUAGE,Field.SUCCESS,Field.ERRORCODE,
            Field.CREATEDON
    );
}
