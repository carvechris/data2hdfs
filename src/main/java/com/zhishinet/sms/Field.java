package com.zhishinet.sms;

import org.apache.storm.tuple.Fields;

public class Field {

    public static final String ID = "id";
    public static final String KEY = "key";
    public static final String MOBILEPHONENO = "mobilePhoneNo";
    public static final String CODE = "code";
    public static final String STATE = "state";
    public static final String RETURNMSG = "returnMsg";
    public static final String POSTTIME = "postTime";
    public static final String CREATEDON = "createdOn";
    public static final String CREATEDBY = "createdBy";
    public static final String MODIFIEDON = "modifiedOn";
    public static final String MODIFIEDBY = "modifiedBy";
    public static final String DELETEDON = "deletedOn";
    public static final String DELETEDBY = "deletedBy";
    public static final String DELETED = "deleted";
    public static final String OPENID = "openId";

    public static final Fields kafkaMessageFields = new Fields(
            Field.ID,Field.KEY,Field.MOBILEPHONENO,Field.CODE,
            Field.STATE,Field.RETURNMSG,Field.POSTTIME,Field.CREATEDON, Field.CREATEDBY,
            Field.DELETED
    );
}
