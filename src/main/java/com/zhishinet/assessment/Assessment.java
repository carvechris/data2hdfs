package com.zhishinet.assessment;

import com.google.common.collect.Lists;

import java.util.List;

public class Assessment implements java.io.Serializable {

    private Integer userId;
    private Integer assessmentId;
    private Integer sessionId;
    private Integer score;
    private List<Question> questions = Lists.newArrayList();

    public Assessment() {
    }

    public Assessment(Integer userId, Integer assessmentId, Integer sessionId, Integer score, List<Question> questions) {
        this.userId = userId;
        this.assessmentId = assessmentId;
        this.sessionId = sessionId;
        this.score = score;
        this.questions = questions;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getAssessmentId() {
        return assessmentId;
    }

    public void setAssessmentId(Integer assessmentId) {
        this.assessmentId = assessmentId;
    }

    public Integer getSessionId() {
        return sessionId;
    }

    public void setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public List<Question> getQuestions() {
        return questions;
    }

    public void setQuestions(List<Question> questions) {
        this.questions = questions;
    }
}
