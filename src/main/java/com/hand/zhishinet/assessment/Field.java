package com.hand.zhishinet.assessment;

import org.apache.storm.tuple.Fields;

/**
 * @author zong.liu01@hand-china.com  2018/9/19 10:56
 * @version 1.0
 * @name zhishinet-bigData
 * @description
 */
public class Field {

    public static Fields getHomeworkAssessmentFields(){
        return new Fields("homeworkAssessmentId","assessmentTitle","tenantId",
                "isTimerOn","timerMode","assessmentQuestions",
                "isDeleted","templateType","assessmentBuilderType",
                "isOptionRandom","minimumPassPercentage","beginDate",
                "endDate","assessmentClassification","duration",
                "allowBack","allowSkip","disableFeedback",
                "assessmentBuilderId","subjectId","isOral",
                "showSubTitle","displayOrder","textbookId",
                "textbookSeriesId","assessmentIntroText","createdBy",
                "createdOn","modifiedBy","modifiedOn",
                "deletedBy","deletedOn");
    }

    public static Fields getHomeworkAssessmentSessionFields(){
        return new Fields("homeworkAssessmentId","assessmentSessionId","sessionId",
                "emendTypeCode","isClose","isRequire",
                "isRequiredEmend", "isDeleted","sessionGroupId",
                "createdBy","createdOn","modifiedBy",
                "modifiedOn", "deletedBy","deletedOn");
    }

    public static Fields getHomeworkSessionUserTrackingFields() {
        return new Fields("homeworkSessionUserTrackingId","sessionId","homeworkAssessmentId","userId","noOfVisits",
                "lastViewedOn","statusId","completedOn","score","percentScore","completeAttempts","beginDate","endDate","timeSpent",
                "interactionTimer","articleLocation","location","isChecked","forLearnerStatus","questionIndexs","emendStatus","IsRequiredEmend",
                "subjectId","readCount","showSubTitle","emendTypeCode","sessionGroupId","displayOrder","createdOn","createdBy","modifiedOn",
                "modifiedBy","deletedOn","deletedBy","deleted");
    }
}
