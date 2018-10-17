package com.hand.zhishinet.assessment.vo;

import java.io.Serializable;
import java.util.Date;

/**
 * @author zong.liu01@hand-china.com  2018/10/16 1:00
 * @version 1.0
 * @name CombineLaunch
 * @description HDFS中的表(UBHomeworkSessionUserTracking)对应的类
 */
public class UBHomeworkSessionUserTracking implements Serializable {

    private static final long serialVersionUID = -434432423112121L;
    private Long homeworkSessionUserTrackingId;
    private Integer sessionId;
    private Long homeworkAssessmentId;
    private Integer userId;
    private Integer noOfVisits;
    private Date lastViewedOn;
    private Integer statusId;
    private Date completedOn;
    private Float score;
    private Float percentScore;
    private Integer completeAttempts;
    private Date beginDate;
    private Date endDate;
    private Long timeSpent;
    private String interactionTimer;
    private Integer articleLocation;
    private String location;
    private Boolean isChecked;
    private Integer forLearnerStatus;
    private String questionIndexs;
    private String emendStatus;
    private Boolean IsRequiredEmend;
    private Integer subjectId;
    private Integer readCount;
    private Boolean showSubTitle;
    private String emendTypeCode;
    private Integer sessionGroupId;
    private Integer displayOrder;
    private Date createdOn;
    private Integer createdBy;
    private Date modifiedOn;
    private Integer modifiedBy;
    private Date deletedOn;
    private Integer deletedBy;
    private Boolean deleted = Boolean.FALSE;

    private String homeworkType;
    private String standardLevel;
    private Double standardConf;
    private String ocrErrorMsg;

    public Long getHomeworkSessionUserTrackingId() {
        return homeworkSessionUserTrackingId;
    }

    public void setHomeworkSessionUserTrackingId(Long homeworkSessionUserTrackingId) {
        this.homeworkSessionUserTrackingId = homeworkSessionUserTrackingId;
    }

    public Integer getSessionId() {
        return sessionId;
    }

    public void setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
    }

    public Long getHomeworkAssessmentId() {
        return homeworkAssessmentId;
    }

    public void setHomeworkAssessmentId(Long homeworkAssessmentId) {
        this.homeworkAssessmentId = homeworkAssessmentId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getNoOfVisits() {
        return noOfVisits;
    }

    public void setNoOfVisits(Integer noOfVisits) {
        this.noOfVisits = noOfVisits;
    }

    public Date getLastViewedOn() {
        return lastViewedOn;
    }

    public void setLastViewedOn(Date lastViewedOn) {
        this.lastViewedOn = lastViewedOn;
    }

    public Integer getStatusId() {
        return statusId;
    }

    public void setStatusId(Integer statusId) {
        this.statusId = statusId;
    }

    public Date getCompletedOn() {
        return completedOn;
    }

    public void setCompletedOn(Date completedOn) {
        this.completedOn = completedOn;
    }

    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }

    public Float getPercentScore() {
        return percentScore;
    }

    public void setPercentScore(Float percentScore) {
        this.percentScore = percentScore;
    }

    public Integer getCompleteAttempts() {
        return completeAttempts;
    }

    public void setCompleteAttempts(Integer completeAttempts) {
        this.completeAttempts = completeAttempts;
    }

    public Date getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Long getTimeSpent() {
        return timeSpent;
    }

    public void setTimeSpent(Long timeSpent) {
        this.timeSpent = timeSpent;
    }

    public String getInteractionTimer() {
        return interactionTimer;
    }

    public void setInteractionTimer(String interactionTimer) {
        this.interactionTimer = interactionTimer;
    }

    public Integer getArticleLocation() {
        return articleLocation;
    }

    public void setArticleLocation(Integer articleLocation) {
        this.articleLocation = articleLocation;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Boolean getChecked() {
        return isChecked;
    }

    public void setChecked(Boolean checked) {
        isChecked = checked;
    }

    public Integer getForLearnerStatus() {
        return forLearnerStatus;
    }

    public void setForLearnerStatus(Integer forLearnerStatus) {
        this.forLearnerStatus = forLearnerStatus;
    }

    public String getQuestionIndexs() {
        return questionIndexs;
    }

    public void setQuestionIndexs(String questionIndexs) {
        this.questionIndexs = questionIndexs;
    }

    public String getEmendStatus() {
        return emendStatus;
    }

    public void setEmendStatus(String emendStatus) {
        this.emendStatus = emendStatus;
    }

    public Boolean getRequiredEmend() {
        return IsRequiredEmend;
    }

    public void setRequiredEmend(Boolean requiredEmend) {
        IsRequiredEmend = requiredEmend;
    }

    public Integer getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(Integer subjectId) {
        this.subjectId = subjectId;
    }

    public Integer getReadCount() {
        return readCount;
    }

    public void setReadCount(Integer readCount) {
        this.readCount = readCount;
    }

    public Boolean getShowSubTitle() {
        return showSubTitle;
    }

    public void setShowSubTitle(Boolean showSubTitle) {
        this.showSubTitle = showSubTitle;
    }

    public String getEmendTypeCode() {
        return emendTypeCode;
    }

    public void setEmendTypeCode(String emendTypeCode) {
        this.emendTypeCode = emendTypeCode;
    }

    public Integer getSessionGroupId() {
        return sessionGroupId;
    }

    public void setSessionGroupId(Integer sessionGroupId) {
        this.sessionGroupId = sessionGroupId;
    }

    public Integer getDisplayOrder() {
        return displayOrder;
    }

    public void setDisplayOrder(Integer displayOrder) {
        this.displayOrder = displayOrder;
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
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public String getHomeworkType() {
        return homeworkType;
    }

    public void setHomeworkType(String homeworkType) {
        this.homeworkType = homeworkType;
    }

    public String getStandardLevel() {
        return standardLevel;
    }

    public void setStandardLevel(String standardLevel) {
        this.standardLevel = standardLevel;
    }

    public Double getStandardConf() {
        return standardConf;
    }

    public void setStandardConf(Double standardConf) {
        this.standardConf = standardConf;
    }

    public String getOcrErrorMsg() {
        return ocrErrorMsg;
    }

    public void setOcrErrorMsg(String ocrErrorMsg) {
        this.ocrErrorMsg = ocrErrorMsg;
    }
}
