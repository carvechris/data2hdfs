package com.zhishinet.sms;

import java.util.Date;

/**
 * @author tomaer
 */
public class UBUserSMSLog implements java.io.Serializable {

    private static final long serialVersionUID = 1115891903247216098L;

    private Long id;
    private String key;
    private String mobilePhoneNo;
    private String code;
    private Integer state;
    private String returnMsg;
    private String postTime;
    private String createdOn;
    private Integer createdBy;
//    private Date modifiedOn;
//    private String modifiedBy;
//    private Date deletedOn;
//    private Integer deletedBy;
    private boolean deleted;
    private String callbackId;
//    private String openId;


    public String getCallbackId() {
        return callbackId;
    }

    public void setCallbackId(String callbackId) {
        this.callbackId = callbackId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getMobilePhoneNo() {
        return mobilePhoneNo;
    }

    public void setMobilePhoneNo(String mobilePhoneNo) {
        this.mobilePhoneNo = mobilePhoneNo;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getState() {
        return state;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public String getReturnMsg() {
        return returnMsg;
    }

    public void setReturnMsg(String returnMsg) {
        this.returnMsg = returnMsg;
    }

    public String getPostTime() {
        return postTime;
    }

    public void setPostTime(String postTime) {
        this.postTime = postTime;
    }

    public String getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(String createdOn) {
        this.createdOn = createdOn;
    }

    public Integer getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(Integer createdBy) {
        this.createdBy = createdBy;
    }

//    public Date getModifiedOn() {
//        return modifiedOn;
//    }
//
//    public void setModifiedOn(Date modifiedOn) {
//        this.modifiedOn = modifiedOn;
//    }
//
//    public String getModifiedBy() {
//        return modifiedBy;
//    }
//
//    public void setModifiedBy(String modifiedBy) {
//        this.modifiedBy = modifiedBy;
//    }
//
//    public Date getDeletedOn() {
//        return deletedOn;
//    }
//
//    public void setDeletedOn(Date deletedOn) {
//        this.deletedOn = deletedOn;
//    }
//
//    public Integer getDeletedBy() {
//        return deletedBy;
//    }
//
//    public void setDeletedBy(Integer deletedBy) {
//        this.deletedBy = deletedBy;
//    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

//    public String getOpenId() {
//        return openId;
//    }
//
//    public void setOpenId(String openId) {
//        this.openId = openId;
//    }
}
