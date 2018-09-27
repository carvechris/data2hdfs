package com.hand.zhishinet.assessment.vo;

import java.io.Serializable;
import java.util.Date;

public class UBHomeworkAssessmentEmendUserInteraction implements Serializable {
    private static final long serialVersionUID = 4429845468703952991L;
    private Long homeworkAssessmentEmendUserInteractionId;
    private Long homeworkSessionUserTrackingId;
    private Long homeworkAssessmentId;
    private Integer questionId;
    private Integer sourceQuestionId;
    private String questionAnswer;
    private Integer correctResponse;
    private String userResponse;
    private Date interactionDate;
    private Integer attemptNo;
    private Integer interactionTimeSpent;
    private Float userScore;
    private String textUserResponse;
    private Boolean feedbackViewed;
    private Date createdOn;
    private Integer createdBy;
    private Date modifiedOn;
    private Integer modifiedBy;
    private Date deletedOn;
    private Integer deletedBy;
    private Boolean isDeleted;

    public Long getHomeworkAssessmentEmendUserInteractionId() {
        return homeworkAssessmentEmendUserInteractionId;
    }

    public void setHomeworkAssessmentEmendUserInteractionId(Long homeworkAssessmentEmendUserInteractionId) {
        this.homeworkAssessmentEmendUserInteractionId = homeworkAssessmentEmendUserInteractionId;
    }

    public Long getHomeworkSessionUserTrackingId() {
        return homeworkSessionUserTrackingId;
    }

    public void setHomeworkSessionUserTrackingId(Long homeworkSessionUserTrackingId) {
        this.homeworkSessionUserTrackingId = homeworkSessionUserTrackingId;
    }

    public Long getHomeworkAssessmentId() {
        return homeworkAssessmentId;
    }

    public void setHomeworkAssessmentId(Long homeworkAssessmentId) {
        this.homeworkAssessmentId = homeworkAssessmentId;
    }

    public Integer getQuestionId() {
        return questionId;
    }

    public void setQuestionId(Integer questionId) {
        this.questionId = questionId;
    }

    public Integer getSourceQuestionId() {
        return sourceQuestionId;
    }

    public void setSourceQuestionId(Integer sourceQuestionId) {
        this.sourceQuestionId = sourceQuestionId;
    }

    public String getQuestionAnswer() {
        return questionAnswer;
    }

    public void setQuestionAnswer(String questionAnswer) {
        this.questionAnswer = questionAnswer;
    }

    public Integer getCorrectResponse() {
        return correctResponse;
    }

    public void setCorrectResponse(Integer correctResponse) {
        this.correctResponse = correctResponse;
    }

    public String getUserResponse() {
        return userResponse;
    }

    public void setUserResponse(String userResponse) {
        this.userResponse = userResponse;
    }

    public Date getInteractionDate() {
        return interactionDate;
    }

    public void setInteractionDate(Date interactionDate) {
        this.interactionDate = interactionDate;
    }

    public Integer getAttemptNo() {
        return attemptNo;
    }

    public void setAttemptNo(Integer attemptNo) {
        this.attemptNo = attemptNo;
    }

    public Integer getInteractionTimeSpent() {
        return interactionTimeSpent;
    }

    public void setInteractionTimeSpent(Integer interactionTimeSpent) {
        this.interactionTimeSpent = interactionTimeSpent;
    }

    public Float getUserScore() {
        return userScore;
    }

    public void setUserScore(Float userScore) {
        this.userScore = userScore;
    }

    public String getTextUserResponse() {
        return textUserResponse;
    }

    public void setTextUserResponse(String textUserResponse) {
        this.textUserResponse = textUserResponse;
    }

    public Boolean getFeedbackViewed() {
        return feedbackViewed;
    }

    public void setFeedbackViewed(Boolean feedbackViewed) {
        this.feedbackViewed = feedbackViewed;
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
}
