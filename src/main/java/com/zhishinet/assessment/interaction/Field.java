package com.zhishinet.assessment.interaction;

import org.apache.storm.tuple.Fields;

public class Field {

    public static final String HOMEWORKASSESSMENTUSERINTERACTIONID = "homeworkAssessmentUserInteractionId", HOMEWORKSESSIONUSERTRACKINGID = "homeworkSessionUserTrackingId";
    public static final String HOMEWORKASSESSMENTID = "homeworkAssessmentId", QUESTIONID = "questionId";
    public static final String CORRECTRESPONSE = "correctResponse", USERRESPONSE = "userResponse";
    public static final String INTERACTIONDATE = "interactionDate", ATTEMPTNO = "attemptNo";
    public static final String INTERACTIONTIMESPENT = "interactionTimeSpent", USERSCORE = "userScore";
    public static final String TEXTUSERRESPONSE = "textUserResponse", FEEDBACKVIEWED = "feedbackViewed";
    public static final String CREATEDON = "createdOn", CREATEDBY = "createdBy";
    public static final String MODIFIEDON = "modifiedOn", MODIFIEDBY = "modifiedBy";
    public static final String DELETEDON = "deletedOn", DELETEDBY = "deletedBy", DELETED = "deleted";
    public static final String QUESTIONANSWER = "questionAnswer", READCOUNT = "readCount";
    public static final String STANDARDSCORE = "standardScore", AUDIOPATH = "audioPath";
    public static final String ORALSCORE = "oralScore", GUESSWORDTIMESPENT = "guessWordTimeSpent";

    public static final Fields kafkaMessageFields = new Fields(
            Field.HOMEWORKASSESSMENTUSERINTERACTIONID, Field.HOMEWORKSESSIONUSERTRACKINGID,
            Field.HOMEWORKASSESSMENTID, Field.QUESTIONID, Field.CORRECTRESPONSE,
            Field.USERRESPONSE, Field.INTERACTIONDATE, Field.ATTEMPTNO, Field.INTERACTIONTIMESPENT, Field.USERSCORE,
            Field.TEXTUSERRESPONSE, Field.FEEDBACKVIEWED, Field.CREATEDON, Field.CREATEDBY, Field.MODIFIEDON, Field.MODIFIEDBY,
            Field.DELETEDON, Field.DELETEDBY, Field.DELETED, Field.QUESTIONANSWER,
            Field.READCOUNT, Field.STANDARDSCORE, Field.AUDIOPATH, Field.ORALSCORE, Field.GUESSWORDTIMESPENT
    );
}

