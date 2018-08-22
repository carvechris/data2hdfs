package com.zhishinet.sms;

public class UBUserSMSLog implements java.io.Serializable {

    private Integer id;
    private String key;
    private String mobilePhoneNo;
    private String code;
    private int state;
    private String returnMsg;
    private String postTime;
    private String createdOn;

    public UBUserSMSLog() {
    }

    public UBUserSMSLog(Integer id, String key, String mobilePhoneNo, String code, int state, String returnMsg, String postTime, String createdOn) {
        this.id = id;
        this.key = key;
        this.mobilePhoneNo = mobilePhoneNo;
        this.code = code;
        this.state = state;
        this.returnMsg = returnMsg;
        this.postTime = postTime;
        this.createdOn = createdOn;
    }

    public Integer getId() {
        return id;
    }

    public UBUserSMSLog setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getKey() {
        return key;
    }

    public UBUserSMSLog setKey(String key) {
        this.key = key;
        return this;
    }

    public String getMobilePhoneNo() {
        return mobilePhoneNo;
    }

    public UBUserSMSLog setMobilePhoneNo(String mobilePhoneNo) {
        this.mobilePhoneNo = mobilePhoneNo;
        return this;
    }

    public String getCode() {
        return code;
    }

    public UBUserSMSLog setCode(String code) {
        this.code = code;
        return this;
    }

    public int getState() {
        return state;
    }

    public UBUserSMSLog setState(int state) {
        this.state = state;
        return this;
    }

    public String getReturnMsg() {
        return returnMsg;
    }

    public UBUserSMSLog setReturnMsg(String returnMsg) {
        this.returnMsg = returnMsg;
        return this;
    }

    public String getPostTime() {
        return postTime;
    }

    public UBUserSMSLog setPostTime(String postTime) {
        this.postTime = postTime;
        return this;
    }

    public String getCreatedOn() {
        return createdOn;
    }

    public UBUserSMSLog setCreatedOn(String createdOn) {
        this.createdOn = createdOn;
        return this;
    }
}
