package com.zhishinet.assessment1.interaction;

/**
 * @author tomaer
 */
public class HomeworkAssessmentUserInteraction implements java.io.Serializable {

    private static final long serialVersionUID = -6381729196769192838L;

    private Integer homeworkAssessmentUserInteractionId;
    private Integer homeworkSessionUserTrackingId;
    private Integer homeworkAssessmentId;
    private Integer questionId;
    private Integer correctResponse;
    private String userResponse;
    private String interactionDate;
    private Integer attemptNo;
    private Integer interactionTimeSpent;
    private Double userScore;
    private String textUserResponse;
    private boolean feedbackViewed;
    private String createdOn;
    private Integer createdBy;
    private String modifiedOn;
    private Integer modifiedBy;
    private String deletedOn;
    private Integer deletedBy;
    private boolean deleted;
    private String questionAnswer;
    private Integer readCount;
    private Integer standardScore;
    private String audioPath;
    private Integer oralScore;
    private Integer guessWordTimeSpent;

    public Integer getHomeworkAssessmentUserInteractionId() {
        return homeworkAssessmentUserInteractionId;
    }

    public void setHomeworkAssessmentUserInteractionId(Integer homeworkAssessmentUserInteractionId) {
        this.homeworkAssessmentUserInteractionId = homeworkAssessmentUserInteractionId;
    }

    public Integer getHomeworkSessionUserTrackingId() {
        return homeworkSessionUserTrackingId;
    }

    public void setHomeworkSessionUserTrackingId(Integer homeworkSessionUserTrackingId) {
        this.homeworkSessionUserTrackingId = homeworkSessionUserTrackingId;
    }

    public Integer getHomeworkAssessmentId() {
        return homeworkAssessmentId;
    }

    public void setHomeworkAssessmentId(Integer homeworkAssessmentId) {
        this.homeworkAssessmentId = homeworkAssessmentId;
    }

    public Integer getQuestionId() {
        return questionId;
    }

    public void setQuestionId(Integer questionId) {
        this.questionId = questionId;
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

    public String getInteractionDate() {
        return interactionDate;
    }

    public void setInteractionDate(String interactionDate) {
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

    public Double getUserScore() {
        return userScore;
    }

    public void setUserScore(Double userScore) {
        this.userScore = userScore;
    }

    public String getTextUserResponse() {
        return textUserResponse;
    }

    public void setTextUserResponse(String textUserResponse) {
        this.textUserResponse = textUserResponse;
    }

    public boolean isFeedbackViewed() {
        return feedbackViewed;
    }

    public void setFeedbackViewed(boolean feedbackViewed) {
        this.feedbackViewed = feedbackViewed;
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

    public String getModifiedOn() {
        return modifiedOn;
    }

    public void setModifiedOn(String modifiedOn) {
        this.modifiedOn = modifiedOn;
    }

    public Integer getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(Integer modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public String getDeletedOn() {
        return deletedOn;
    }

    public void setDeletedOn(String deletedOn) {
        this.deletedOn = deletedOn;
    }

    public Integer getDeletedBy() {
        return deletedBy;
    }

    public void setDeletedBy(Integer deletedBy) {
        this.deletedBy = deletedBy;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public String getQuestionAnswer() {
        return questionAnswer;
    }

    public void setQuestionAnswer(String questionAnswer) {
        this.questionAnswer = questionAnswer;
    }

    public Integer getReadCount() {
        return readCount;
    }

    public void setReadCount(Integer readCount) {
        this.readCount = readCount;
    }

    public Integer getStandardScore() {
        return standardScore;
    }

    public void setStandardScore(Integer standardScore) {
        this.standardScore = standardScore;
    }

    public String getAudioPath() {
        return audioPath;
    }

    public void setAudioPath(String audioPath) {
        this.audioPath = audioPath;
    }

    public Integer getOralScore() {
        return oralScore;
    }

    public void setOralScore(Integer oralScore) {
        this.oralScore = oralScore;
    }

    public Integer getGuessWordTimeSpent() {
        return guessWordTimeSpent;
    }

    public void setGuessWordTimeSpent(Integer guessWordTimeSpent) {
        this.guessWordTimeSpent = guessWordTimeSpent;
    }
}
