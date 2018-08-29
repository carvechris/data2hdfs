package com.zhishinet.assessment;

public class Question implements java.io.Serializable {

    private Integer questionId;
    private Integer correctReponse;
    private Integer userResponse;

    public Question() {
    }

    public Question(Integer questionId, Integer correctReponse, Integer userResponse) {
        this.questionId = questionId;
        this.correctReponse = correctReponse;
        this.userResponse = userResponse;
    }

    public Integer getQuestionId() {
        return questionId;
    }

    public void setQuestionId(Integer questionId) {
        this.questionId = questionId;
    }

    public Integer getCorrectReponse() {
        return correctReponse;
    }

    public void setCorrectReponse(Integer correctReponse) {
        this.correctReponse = correctReponse;
    }

    public Integer getUserResponse() {
        return userResponse;
    }

    public void setUserResponse(Integer userResponse) {
        this.userResponse = userResponse;
    }
}
