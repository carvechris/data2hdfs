package com.hand.zhishinet.assessment.vo;

import java.util.Date;

/**
 * @author zong.liu01@hand-china.com  2018/10/16 1:00
 * @version 1.0
 * @name CombineLaunch
 * @description HDFS中的表(UBHomeworkAssessmentSession)对应的类
 */
public class UBHomeworkAssessmentSession implements java.io.Serializable {

    private static final long serialVersionUID = -121111121467098L;
    private Long assessmentSessionId;
    private Long homeworkAssessmentId;
    private Integer sessionGroupId;
    private Boolean isRequiredEmend;
    private String emendTypeCode;
    private Boolean isRequire;
    private Integer sessionId;
    private Boolean isClose;
    private String homeworkType;

    protected Date createdOn;
    protected Integer createdBy;
    protected Date modifiedOn;
    protected Integer modifiedBy;
    protected Date deletedOn;
    protected Integer deletedBy;
    protected Boolean isDeleted;

    public Long getAssessmentSessionId() {
        return assessmentSessionId;
    }

    public void setAssessmentSessionId(Long assessmentSessionId) {
        this.assessmentSessionId = assessmentSessionId;
    }

    public Long getHomeworkAssessmentId() {
        return homeworkAssessmentId;
    }

    public void setHomeworkAssessmentId(Long homeworkAssessmentId) {
        this.homeworkAssessmentId = homeworkAssessmentId;
    }

    public Integer getSessionGroupId() {
        return sessionGroupId;
    }

    public void setSessionGroupId(Integer sessionGroupId) {
        this.sessionGroupId = sessionGroupId;
    }

    public Boolean getRequiredEmend() {
        return isRequiredEmend;
    }

    public void setRequiredEmend(Boolean requiredEmend) {
        isRequiredEmend = requiredEmend;
    }

    public String getEmendTypeCode() {
        return emendTypeCode;
    }

    public void setEmendTypeCode(String emendTypeCode) {
        this.emendTypeCode = emendTypeCode;
    }

    public Boolean getRequire() {
        return isRequire;
    }

    public void setRequire(Boolean require) {
        isRequire = require;
    }

    public Integer getSessionId() {
        return sessionId;
    }

    public void setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
    }

    public Boolean getClose() {
        return isClose;
    }

    public void setClose(Boolean close) {
        isClose = close;
    }

    public Date getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
    }

    public Integer getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(Integer createdBy) {
        this.createdBy = createdBy;
    }

    public Date getModifiedOn() {
        return modifiedOn;
    }

    public void setModifiedOn(Date modifiedOn) {
        this.modifiedOn = modifiedOn;
    }

    public Integer getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(Integer modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public Date getDeletedOn() {
        return deletedOn;
    }

    public void setDeletedOn(Date deletedOn) {
        this.deletedOn = deletedOn;
    }

    public Integer getDeletedBy() {
        return deletedBy;
    }

    public void setDeletedBy(Integer deletedBy) {
        this.deletedBy = deletedBy;
    }

    public Boolean getDeleted() {
        return isDeleted;
    }

    public void setDeleted(Boolean deleted) {
        isDeleted = deleted;
    }

    public String getHomeworkType() {
        return homeworkType;
    }

    public void setHomeworkType(String homeworkType) {
        this.homeworkType = homeworkType;
    }
}
