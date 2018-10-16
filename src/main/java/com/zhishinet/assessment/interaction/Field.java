package com.zhishinet.assessment.interaction;

import org.apache.storm.tuple.Fields;

public class Field {

    public static final String HOMEWORKASSESSMENTUSERINTERACTIONID = "homeworkAssessmentUserInteractionId";
    public static final String HOMEWORKSESSIONUSERTRACKINGID = "homeworkSessionUserTrackingId";
    public static final String HOMEWORKASSESSMENTID = "homeworkAssessmentId";
    public static final String QUESTIONID = "questionId";
    public static final String CORRECTRESPONSE = "correctResponse";
    public static final String USERRESPONSE = "userResponse";
    public static final String INTERACTIONDATE = "INTERACTIONDATE";
    public static final String ATTEMPTNO = "attemptNo";
    public static final String INTERACTIONTIMESPENT = "INTERACTIONTIMESPENT";
    public static final String USERSCORE = "userScore";
    public static final String TEXTUSERRESPONSE = "textUserResponse";
    public static final String FEEDBACKVIEWED = "feedbackViewed";
    public static final String CREATEDON = "createdOn";
    public static final String CREATEDBY = "createdBy";
    public static final String MODIFIEDON = "modifiedOn";
    public static final String MODIFIEDBY = "modifiedBy";
    public static final String DELETEDON = "deletedOn";
    public static final String DELETEDBY = "deletedBy";
    public static final String DELETED = "deleted";
    public static final String QUESTIONANSWER = "questionAnswer";
    public static final String READCOUNT = "readCount";
    public static final String STANDARDSCORE = "standardScore";
    public static final String AUDIOPATH = "audioPath";
    public static final String ORALSCORE = "oralScore";
    public static final String GUESSWORDTIMESPENT = "guessWordTimeSpent";
    public static final String SESSIONID = "sessionId";

    public static final Fields kafkaMessageFields = new Fields(
            Field.HOMEWORKASSESSMENTUSERINTERACTIONID,
            Field.HOMEWORKSESSIONUSERTRACKINGID,
            Field.HOMEWORKASSESSMENTID,
            Field.QUESTIONID,
            Field.CORRECTRESPONSE,
            Field.USERRESPONSE,
            Field.INTERACTIONDATE,
            Field.ATTEMPTNO,
            Field.INTERACTIONTIMESPENT,
            Field.USERSCORE,
            Field.TEXTUSERRESPONSE,
            Field.FEEDBACKVIEWED,
            Field.CREATEDON,
            Field.CREATEDBY,
            Field.MODIFIEDON,
            Field.MODIFIEDBY,
            Field.DELETEDON,
            Field.DELETEDBY,
            Field.DELETED,
            Field.QUESTIONANSWER,
            Field.READCOUNT,
            Field.STANDARDSCORE,
            Field.AUDIOPATH,
            Field.ORALSCORE,
            Field.GUESSWORDTIMESPENT,
            Field.SESSIONID
    );
}

