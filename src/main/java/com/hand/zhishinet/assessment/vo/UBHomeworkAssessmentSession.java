package com.hand.zhishinet.assessment.vo;

/**
 * @author zong.liu01@hand-china.com  2018/9/19 13:49
 * @version 1.0
 * @name zhishinet-bigData
 * @description
 */
public class UBHomeworkAssessmentSession extends BaseDTO implements java.io.Serializable {

    private static final long serialVersionUID = 11158919214213098L;

    private Long assessmentSessionId;
    private Long homeworkAssessmentId;
    private Integer sessionGroupId;
    private Boolean isRequiredEmend;
    private String emendTypeCode;
    private Boolean isRequire;
    private Integer sessionId;
    private Boolean isClose;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

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
}
