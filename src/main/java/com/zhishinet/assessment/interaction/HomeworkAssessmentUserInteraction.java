package com.zhishinet.assessment.interaction;

/**
 * @author tomaer
 */
public class HomeworkAssessmentUserInteraction implements java.io.Serializable {

    private static final long serialVersionUID = -6381729196769192838L;

    private Long homeworkAssessmentUserInteractionId;
    private Long homeworkSessionUserTrackingId;
    private Long homeworkAssessmentId;
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
    private Integer sessionId;

    public Long getHomeworkAssessmentUserInteractionId() {
        return homeworkAssessmentUserInteractionId;
    }

    public HomeworkAssessmentUserInteraction setHomeworkAssessmentUserInteractionId(Long homeworkAssessmentUserInteractionId) {
        this.homeworkAssessmentUserInteractionId = homeworkAssessmentUserInteractionId;
        return this;
    }

    public Long getHomeworkSessionUserTrackingId() {
        return homeworkSessionUserTrackingId;
    }

    public HomeworkAssessmentUserInteraction setHomeworkSessionUserTrackingId(Long homeworkSessionUserTrackingId) {
        this.homeworkSessionUserTrackingId = homeworkSessionUserTrackingId;
        return this;
    }

    public Long getHomeworkAssessmentId() {
        return homeworkAssessmentId;
    }

    public HomeworkAssessmentUserInteraction setHomeworkAssessmentId(Long homeworkAssessmentId) {
        this.homeworkAssessmentId = homeworkAssessmentId;
        return this;
    }

    public Integer getQuestionId() {
        return questionId;
    }

    public HomeworkAssessmentUserInteraction setQuestionId(Integer questionId) {
        this.questionId = questionId;
        return this;
    }

    public Integer getCorrectResponse() {
        return correctResponse;
    }

    public HomeworkAssessmentUserInteraction setCorrectResponse(Integer correctResponse) {
        this.correctResponse = correctResponse;
        return this;
    }

    public String getUserResponse() {
        return userResponse;
    }

    public HomeworkAssessmentUserInteraction setUserResponse(String userResponse) {
        this.userResponse = userResponse;
        return this;
    }

    public String getInteractionDate() {
        return interactionDate;
    }

    public HomeworkAssessmentUserInteraction setInteractionDate(String interactionDate) {
        this.interactionDate = interactionDate;
        return this;
    }

    public Integer getAttemptNo() {
        return attemptNo;
    }

    public HomeworkAssessmentUserInteraction setAttemptNo(Integer attemptNo) {
        this.attemptNo = attemptNo;
        return this;
    }

    public Integer getInteractionTimeSpent() {
        return interactionTimeSpent;
    }

    public HomeworkAssessmentUserInteraction setInteractionTimeSpent(Integer interactionTimeSpent) {
        this.interactionTimeSpent = interactionTimeSpent;
        return this;
    }

    public Double getUserScore() {
        return userScore;
    }

    public HomeworkAssessmentUserInteraction setUserScore(Double userScore) {
        this.userScore = userScore;
        return this;
    }

    public String getTextUserResponse() {
        return textUserResponse;
    }

    public HomeworkAssessmentUserInteraction setTextUserResponse(String textUserResponse) {
        this.textUserResponse = textUserResponse;
        return this;
    }

    public boolean isFeedbackViewed() {
        return feedbackViewed;
    }

    public HomeworkAssessmentUserInteraction setFeedbackViewed(boolean feedbackViewed) {
        this.feedbackViewed = feedbackViewed;
        return this;
    }

    public String getCreatedOn() {
        return createdOn;
    }

    public HomeworkAssessmentUserInteraction setCreatedOn(String createdOn) {
        this.createdOn = createdOn;
        return this;
    }

    public Integer getCreatedBy() {
        return createdBy;
    }

    public HomeworkAssessmentUserInteraction setCreatedBy(Integer createdBy) {
        this.createdBy = createdBy;
        return this;
    }

    public String getModifiedOn() {
        return modifiedOn;
    }

    public HomeworkAssessmentUserInteraction setModifiedOn(String modifiedOn) {
        this.modifiedOn = modifiedOn;
        return this;
    }

    public Integer getModifiedBy() {
        return modifiedBy;
    }

    public HomeworkAssessmentUserInteraction setModifiedBy(Integer modifiedBy) {
        this.modifiedBy = modifiedBy;
        return this;
    }

    public String getDeletedOn() {
        return deletedOn;
    }

    public HomeworkAssessmentUserInteraction setDeletedOn(String deletedOn) {
        this.deletedOn = deletedOn;
        return this;
    }

    public Integer getDeletedBy() {
        return deletedBy;
    }

    public HomeworkAssessmentUserInteraction setDeletedBy(Integer deletedBy) {
        this.deletedBy = deletedBy;
        return this;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public HomeworkAssessmentUserInteraction setDeleted(boolean deleted) {
        this.deleted = deleted;
        return this;
    }

    public String getQuestionAnswer() {
        return questionAnswer;
    }

    public HomeworkAssessmentUserInteraction setQuestionAnswer(String questionAnswer) {
        this.questionAnswer = questionAnswer;
        return this;
    }

    public Integer getReadCount() {
        return readCount;
    }

    public HomeworkAssessmentUserInteraction setReadCount(Integer readCount) {
        this.readCount = readCount;
        return this;
    }

    public Integer getStandardScore() {
        return standardScore;
    }

    public HomeworkAssessmentUserInteraction setStandardScore(Integer standardScore) {
        this.standardScore = standardScore;
        return this;
    }

    public String getAudioPath() {
        return audioPath;
    }

    public HomeworkAssessmentUserInteraction setAudioPath(String audioPath) {
        this.audioPath = audioPath;
        return this;
    }

    public Integer getOralScore() {
        return oralScore;
    }

    public HomeworkAssessmentUserInteraction setOralScore(Integer oralScore) {
        this.oralScore = oralScore;
        return this;
    }

    public Integer getGuessWordTimeSpent() {
        return guessWordTimeSpent;
    }

    public HomeworkAssessmentUserInteraction setGuessWordTimeSpent(Integer guessWordTimeSpent) {
        this.guessWordTimeSpent = guessWordTimeSpent;
        return this;
    }

    public Integer getSessionId() {
        return sessionId;
    }

    public HomeworkAssessmentUserInteraction setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
        return this;
    }
}
