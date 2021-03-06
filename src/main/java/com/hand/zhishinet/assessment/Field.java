package com.hand.zhishinet.assessment;

import org.apache.storm.tuple.Fields;

/**
 * @author zong.liu01@hand-china.com  2018/9/19 10:56
 * @version 1.0
 * @name zhishinet-bigData
 * @description
 */
public class Field {

    public static final String ASSESSMENTID = "AssessmentId";
    public static final String SESSIONID = "SessionId";
    public static final String SCORE = "Score";
    public static final String USERID = "UserId";
    public static final String SUM = "Sum";
    public static final String COUNT = "Count";
    public static final String SESSIONUSERTRACKINGID = "SessionUserTrackingId";
    public static final String SUBJECT_ID = "SubjectId";

    public static Fields getHomeworkAssessmentFields() {
        return new Fields("homeworkAssessmentId", "assessmentTitle", "tenantId",
                "isTimerOn", "timerMode", "assessmentQuestions",
                "isDeleted", "templateType", "assessmentBuilderType",
                "isOptionRandom", "minimumPassPercentage", "beginDate",
                "endDate", "assessmentClassification", "duration",
                "allowBack", "allowSkip", "disableFeedback",
                "assessmentBuilderId", "subjectId", "isOral",
                "showSubTitle", "displayOrder", "textbookId",
                "textbookSeriesId", "assessmentIntroText", "createdBy",
                "createdOn", "modifiedBy", "modifiedOn",
                "deletedBy", "deletedOn", "homeworkType",
                "isQuestionRandom");
    }

    public static Fields getHomeworkAssessmentSessionFields() {
        return new Fields("homeworkAssessmentId", "assessmentSessionId", "sessionId",
                "emendTypeCode", "isClose", "isRequire",
                "isRequiredEmend", "isDeleted", "sessionGroupId",
                "createdBy", "createdOn", "modifiedBy",
                "modifiedOn", "deletedBy", "deletedOn", "homeworkType");
    }

    public static Fields getHomeworkSessionUserTrackingFields() {
        return new Fields("homeworkSessionUserTrackingId", "sessionId", "homeworkAssessmentId", "userId", "noOfVisits",
                "lastViewedOn", "statusId", "completedOn", "score", "percentScore", "completeAttempts", "beginDate", "endDate", "timeSpent",
                "interactionTimer", "emendStatus", "IsRequiredEmend",
                "subjectId", "readCount", "showSubTitle", "emendTypeCode", "sessionGroupId", "displayOrder", "createdOn", "createdBy", "modifiedOn",
                "modifiedBy", "deletedOn", "deletedBy", "deleted", "homeworkType", "standardLevel", "standardConf", "ocrErrorMsg");
    }

    public static Fields getUBHomeworkSessionUserTrackingAttemptDetailFields() {
        return new Fields("homeworkSessionUserTrackingAttemptDetailId",
                "homeworkSessionUserTrackingId",
                "homeworkAssessmentId",
                "attemptNumber",
                "noOfVisits",
                "timeSpent",
                "statusId",
                "completedOn",
                "score",
                "percentScore",
                "assessmentDifficulty",
                "readCount",
                "createdOn",
                "createdBy",
                "modifiedOn",
                "modifiedBy",
                "deletedOn",
                "deletedBy",
                "deleted",
                "sessionId");
    }

    /**
     * SessionUserTracking  AvgScore字段
     *
     * @return
     */
    public static Fields getSessionUserTrackingAvgScoreFields() {
        return new Fields("sessionId", "homeworkAssessmentId", "score");
    }
}
