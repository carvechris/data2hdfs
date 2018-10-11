package com.hand.zhishinet.assessment.vo;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>Title:  zhishinet-app-student <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/10/11 14:11
 */
public class UBHomeworkSessionUserTrackingAttemptDetail implements Serializable{

    private Long homeworksessionUserTrackingAttemptDetailId;
    private Long homeworkSessionUserTrackingId;
    private Long homeworkAssessmentId;
    private Integer attemptNumber;
    private Integer noOfVisits;
    private Integer timeSpent;
    private Integer statusId;
    private Date completedOn;
    private Float score;
    private Float percentScore;
    private Float assessmentDifficulty;
    private Integer readCount;
    private Date createdOn;
    private Integer createdBy;
    private Date modifiedOn;
    private Integer modifiedBy;
    private Date deletedOn;
    private Integer deletedBy;
    private Boolean deleted = false;
    private Integer sessionId;

    public Long getHomeworksessionUserTrackingAttemptDetailId() {
        return homeworksessionUserTrackingAttemptDetailId;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setHomeworksessionUserTrackingAttemptDetailId(Long homeworksessionUserTrackingAttemptDetailId) {
        this.homeworksessionUserTrackingAttemptDetailId = homeworksessionUserTrackingAttemptDetailId;
        return this;
    }

    public Long getHomeworkSessionUserTrackingId() {
        return homeworkSessionUserTrackingId;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setHomeworkSessionUserTrackingId(Long homeworkSessionUserTrackingId) {
        this.homeworkSessionUserTrackingId = homeworkSessionUserTrackingId;
        return this;
    }

    public Long getHomeworkAssessmentId() {
        return homeworkAssessmentId;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setHomeworkAssessmentId(Long homeworkAssessmentId) {
        this.homeworkAssessmentId = homeworkAssessmentId;
        return this;
    }

    public Integer getAttemptNumber() {
        return attemptNumber;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setAttemptNumber(Integer attemptNumber) {
        this.attemptNumber = attemptNumber;
        return this;
    }

    public Integer getNoOfVisits() {
        return noOfVisits;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setNoOfVisits(Integer noOfVisits) {
        this.noOfVisits = noOfVisits;
        return this;
    }

    public Integer getTimeSpent() {
        return timeSpent;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setTimeSpent(Integer timeSpent) {
        this.timeSpent = timeSpent;
        return this;
    }

    public Integer getStatusId() {
        return statusId;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setStatusId(Integer statusId) {
        this.statusId = statusId;
        return this;
    }

    public Date getCompletedOn() {
        return completedOn;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setCompletedOn(Date completedOn) {
        this.completedOn = completedOn;
        return this;
    }

    public Float getScore() {
        return score;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setScore(Float score) {
        this.score = score;
        return this;
    }

    public Float getPercentScore() {
        return percentScore;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setPercentScore(Float percentScore) {
        this.percentScore = percentScore;
        return this;
    }

    public Float getAssessmentDifficulty() {
        return assessmentDifficulty;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setAssessmentDifficulty(Float assessmentDifficulty) {
        this.assessmentDifficulty = assessmentDifficulty;
        return this;
    }

    public Integer getReadCount() {
        return readCount;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setReadCount(Integer readCount) {
        this.readCount = readCount;
        return this;
    }

    public Date getCreatedOn() {
        return createdOn;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
        return this;
    }

    public Integer getCreatedBy() {
        return createdBy;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setCreatedBy(Integer createdBy) {
        this.createdBy = createdBy;
        return this;
    }

    public Date getModifiedOn() {
        return modifiedOn;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setModifiedOn(Date modifiedOn) {
        this.modifiedOn = modifiedOn;
        return this;
    }

    public Integer getModifiedBy() {
        return modifiedBy;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setModifiedBy(Integer modifiedBy) {
        this.modifiedBy = modifiedBy;
        return this;
    }

    public Date getDeletedOn() {
        return deletedOn;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setDeletedOn(Date deletedOn) {
        this.deletedOn = deletedOn;
        return this;
    }

    public Integer getDeletedBy() {
        return deletedBy;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setDeletedBy(Integer deletedBy) {
        this.deletedBy = deletedBy;
        return this;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setDeleted(Boolean deleted) {
        this.deleted = deleted;
        return this;
    }

    public Integer getSessionId() {
        return sessionId;
    }

    public UBHomeworkSessionUserTrackingAttemptDetail setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
        return this;
    }
}
