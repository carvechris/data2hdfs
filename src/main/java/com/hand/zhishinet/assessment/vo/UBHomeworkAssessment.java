package com.hand.zhishinet.assessment.vo;

import java.util.Date;

/**
 * @author zong.liu01@hand-china.com  2018/10/16 1:00
 * @version 1.0
 * @name CombineLaunch
 * @description HDFS中的表(UBHomeworkAssessment)对应的类
 */
public class UBHomeworkAssessment implements java.io.Serializable {

    private static final long serialVersionUID = -2121221213422L;

    private Long homeworkAssessmentId;
    private String title;
    private String introText;
    private Integer tenantId;
    private Boolean isTimerOn;
    private Integer timerMode;
    private Integer minimumPassPercentage;
    private Integer assessmentQuestions;
    private Date beginDate;
    private Date endDate;
    private Long assessmentBuilderId;
    private Integer templateType;
    private Boolean isOptionRandom;
    private Integer assessmentClassification;
    private Integer duration;
    private Boolean allowBack;
    private Boolean allowSkip;
    private Boolean disableFeedback;
    private Integer assessmentBuilderType;
    private Integer subjectId;
    private Boolean isOral;
    private Boolean showSubTitle;
    private Integer displayOrder;
    private Integer textbookSeriesId;
    private Integer textbookId;
    private String homeworkType;
    private Boolean isQuestionRandom;

    protected Date createdOn;
    protected Integer createdBy;
    protected Date modifiedOn;
    protected Integer modifiedBy;
    protected Date deletedOn;
    protected Integer deletedBy;
    protected Boolean isDeleted;

    public Long getHomeworkAssessmentId() {
        return homeworkAssessmentId;
    }

    public void setHomeworkAssessmentId(Long homeworkAssessmentId) {
        this.homeworkAssessmentId = homeworkAssessmentId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getIntroText() {
        return introText;
    }

    public void setIntroText(String introText) {
        this.introText = introText;
    }

    public Integer getTenantId() {
        return tenantId;
    }

    public void setTenantId(Integer tenantId) {
        this.tenantId = tenantId;
    }

    public Boolean getTimerOn() {
        return isTimerOn;
    }

    public void setTimerOn(Boolean timerOn) {
        isTimerOn = timerOn;
    }

    public Integer getTimerMode() {
        return timerMode;
    }

    public void setTimerMode(Integer timerMode) {
        this.timerMode = timerMode;
    }

    public Integer getMinimumPassPercentage() {
        return minimumPassPercentage;
    }

    public void setMinimumPassPercentage(Integer minimumPassPercentage) {
        this.minimumPassPercentage = minimumPassPercentage;
    }

    public Integer getAssessmentQuestions() {
        return assessmentQuestions;
    }

    public void setAssessmentQuestions(Integer assessmentQuestions) {
        this.assessmentQuestions = assessmentQuestions;
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

    public Long getAssessmentBuilderId() {
        return assessmentBuilderId;
    }

    public void setAssessmentBuilderId(Long assessmentBuilderId) {
        this.assessmentBuilderId = assessmentBuilderId;
    }

    public Integer getTemplateType() {
        return templateType;
    }

    public void setTemplateType(Integer templateType) {
        this.templateType = templateType;
    }

    public Boolean getOptionRandom() {
        return isOptionRandom;
    }

    public void setOptionRandom(Boolean optionRandom) {
        isOptionRandom = optionRandom;
    }

    public Integer getAssessmentClassification() {
        return assessmentClassification;
    }

    public void setAssessmentClassification(Integer assessmentClassification) {
        this.assessmentClassification = assessmentClassification;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public Boolean getAllowBack() {
        return allowBack;
    }

    public void setAllowBack(Boolean allowBack) {
        this.allowBack = allowBack;
    }

    public Boolean getAllowSkip() {
        return allowSkip;
    }

    public void setAllowSkip(Boolean allowSkip) {
        this.allowSkip = allowSkip;
    }

    public Boolean getDisableFeedback() {
        return disableFeedback;
    }

    public void setDisableFeedback(Boolean disableFeedback) {
        this.disableFeedback = disableFeedback;
    }

    public Integer getAssessmentBuilderType() {
        return assessmentBuilderType;
    }

    public void setAssessmentBuilderType(Integer assessmentBuilderType) {
        this.assessmentBuilderType = assessmentBuilderType;
    }

    public Integer getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(Integer subjectId) {
        this.subjectId = subjectId;
    }

    public Boolean getOral() {
        return isOral;
    }

    public void setOral(Boolean oral) {
        isOral = oral;
    }

    public Boolean getShowSubTitle() {
        return showSubTitle;
    }

    public void setShowSubTitle(Boolean showSubTitle) {
        this.showSubTitle = showSubTitle;
    }

    public Integer getDisplayOrder() {
        return displayOrder;
    }

    public void setDisplayOrder(Integer displayOrder) {
        this.displayOrder = displayOrder;
    }

    public Integer getTextbookSeriesId() {
        return textbookSeriesId;
    }

    public void setTextbookSeriesId(Integer textbookSeriesId) {
        this.textbookSeriesId = textbookSeriesId;
    }

    public Integer getTextbookId() {
        return textbookId;
    }

    public void setTextbookId(Integer textbookId) {
        this.textbookId = textbookId;
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

    public Boolean getQuestionRandom() {
        return isQuestionRandom;
    }

    public void setQuestionRandom(Boolean questionRandom) {
        isQuestionRandom = questionRandom;
    }
}
