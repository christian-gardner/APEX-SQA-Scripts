set lines 180
set pages 400
set echo on
set feedback on

spool rdm_rfc_18029_build.log

SET DEFINE OFF;

begin 


    begin

    execute immediate 'drop index RDM.SQA_TD_DATA_BATCH';

    exception 
            when others then
            null;
    end;




 EXECUTE IMMEDIATE 'CREATE INDEX RDM.SQA_TD_DATA_BATCH ON RDM.SQA_TD_DATA ( CONTRACTOR, REPORT_SEGMENT, COMPLETED_DT)';

EXCEPTION 
      WHEN OTHERS THEN
      NULL;
END;
/

BEGIN

   DBMS_SCHEDULER.DROP_JOB('RDM.ARCH_SQA_DATA');
   

EXCEPTION 
    WHEN OTHERS THEN 
    NULL;
END;
/


BEGIN
  SYS.DBMS_SCHEDULER.CREATE_JOB
    (
       job_name        => 'RDM.ARCH_SQA_DATA'
      ,start_date      => TO_TIMESTAMP_TZ('2017/05/03 17:00:00.000000 -04:00','yyyy/mm/dd hh24:mi:ss.ff tzr')
      ,repeat_interval => 'FREQ=WEEKLY;BYDAY=MON,TUE,WED,THU,FRI,SAT,SUN;BYHOUR=01'
      ,end_date        => NULL
      ,job_class       => 'DEFAULT_JOB_CLASS'
      ,job_type        => 'STORED_PROCEDURE'
      ,job_action      => 'SQA_LOVS.ARCHIVE_SQA_DATA'
      ,comments        => 'Refresh RDM Schema'
    );
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.ARCH_SQA_DATA'
     ,attribute => 'RESTARTABLE'
     ,value     => FALSE);
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.ARCH_SQA_DATA'
     ,attribute => 'LOGGING_LEVEL'
     ,value     => SYS.DBMS_SCHEDULER.LOGGING_OFF);
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL
    ( name      => 'RDM.ARCH_SQA_DATA'
     ,attribute => 'MAX_FAILURES');
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL
    ( name      => 'RDM.ARCH_SQA_DATA'
     ,attribute => 'MAX_RUNS');
  BEGIN
    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
      ( name      => 'RDM.ARCH_SQA_DATA'
       ,attribute => 'STOP_ON_WINDOW_CLOSE'
       ,value     => FALSE);
  EXCEPTION
    -- could fail if program is of type EXECUTABLE...
    WHEN OTHERS THEN
      NULL;
  END;
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.ARCH_SQA_DATA'
     ,attribute => 'JOB_PRIORITY'
     ,value     => 3);
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL
    ( name      => 'RDM.ARCH_SQA_DATA'
     ,attribute => 'SCHEDULE_LIMIT');
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.ARCH_SQA_DATA'
     ,attribute => 'AUTO_DROP'
     ,value     => TRUE);

  SYS.DBMS_SCHEDULER.ENABLE
    (name                  => 'RDM.ARCH_SQA_DATA');
END;
/


BEGIN

   DBMS_SCHEDULER.DROP_JOB('RDM.PURGE_SQA_TD_DATA');
   

EXCEPTION 
    WHEN OTHERS THEN 
    NULL;
END;
/


BEGIN
  SYS.DBMS_SCHEDULER.CREATE_JOB
    (
       job_name        => 'RDM.PURGE_SQA_TD_DATA'
      ,start_date      => TO_TIMESTAMP_TZ('2017/05/03 17:00:00.000000 -04:00','yyyy/mm/dd hh24:mi:ss.ff tzr')
      ,repeat_interval => 'FREQ=WEEKLY;BYDAY=MON,TUE,WED,THU,FRI,SAT,SUN;BYHOUR=02'
      ,end_date        => NULL
      ,job_class       => 'DEFAULT_JOB_CLASS'
      ,job_type        => 'STORED_PROCEDURE'
      ,job_action      => 'SQA_LOVS.REMOVE_SQA_ETL_DATA'
      ,comments        => 'Refresh RDM Schema'
    );
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.PURGE_SQA_TD_DATA'
     ,attribute => 'RESTARTABLE'
     ,value     => FALSE);
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.PURGE_SQA_TD_DATA'
     ,attribute => 'LOGGING_LEVEL'
     ,value     => SYS.DBMS_SCHEDULER.LOGGING_OFF);
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL
    ( name      => 'RDM.PURGE_SQA_TD_DATA'
     ,attribute => 'MAX_FAILURES');
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL
    ( name      => 'RDM.PURGE_SQA_TD_DATA'
     ,attribute => 'MAX_RUNS');
  BEGIN
    SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
      ( name      => 'RDM.PURGE_SQA_TD_DATA'
       ,attribute => 'STOP_ON_WINDOW_CLOSE'
       ,value     => FALSE);
  EXCEPTION
    -- could fail if program is of type EXECUTABLE...
    WHEN OTHERS THEN
      NULL;
  END;
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.PURGE_SQA_TD_DATA'
     ,attribute => 'JOB_PRIORITY'
     ,value     => 3);
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE_NULL
    ( name      => 'RDM.PURGE_SQA_TD_DATA'
     ,attribute => 'SCHEDULE_LIMIT');
  SYS.DBMS_SCHEDULER.SET_ATTRIBUTE
    ( name      => 'RDM.PURGE_SQA_TD_DATA'
     ,attribute => 'AUTO_DROP'
     ,value     => TRUE);

  SYS.DBMS_SCHEDULER.ENABLE
    (name   => 'RDM.PURGE_SQA_TD_DATA');
END;
/

CREATE OR REPLACE TRIGGER SQA_TD_DATA_SEQ_BI
BEFORE INSERT ON RDM.SQA_TD_DATA
FOR EACH ROW
DECLARE
  P_ID                number;
  P_CNT               NUMBER;
  P_CNT_2             NUMBER;
  P_CLIENT            VARCHAR2 (10 BYTE);
  P_WORK_CODE         VARCHAR2 (10 BYTE);
  P_WORK_GROUP        VARCHAR2 (20 BYTE);
  P_LOAN_TYPE         VARCHAR2 (10 BYTE);
  NBR_QS              NUMBER;
  SQL_STMP            VARCHAR2 (1000 BYTE);
  P_STANDING          NUMBER;
  MESSAGE             VARCHAR2(300 BYTE);
  P_REPORT_SEGMENT    VARCHAR2(100 BYTE);
  P_CONTRACTOR        VARCHAR2(100 BYTE);
  P_COMPLETEDDATE     DATE;
  START_CHECK_DATE    DATE;
  GV_CURRENT_DATE     DATE;
  GV_TESTING          NUMBER;
  BAD_DATA         EXCEPTION;

BEGIN

          SELECT NVL(MAX(STANDING),0)
            INTO  P_STANDING
            FROM SQA_VENDOR_LIST;

          SELECT VARIABLE_VALUE
           INTO  GV_TESTING
           FROM  SQA_SYS_VARIABLES
          WHERE VARIABLE_NAME = 'TESTING';


         IF ( GV_TESTING = 1 ) THEN

                SELECT TO_DATE(VARIABLE_VALUE,'MM/DD/YYYY')
                 INTO  GV_CURRENT_DATE
                 FROM  SQA_SYS_VARIABLES
                WHERE VARIABLE_NAME = 'Current_date';
         ELSE
                SELECT  TRUNC(SYSDATE)
                INTO    GV_CURRENT_DATE
                FROM  DUAL;
         END IF;

        P_CLIENT         := :new.CLIENT;
        P_WORK_CODE      := :new.work_code;
        P_LOAN_TYPE      := :new.LOAN_TYPE;
        P_CONTRACTOR     := :new.CONTRACTOR;
        P_REPORT_SEGMENT := :new.REPORT_SEGMENT;
        P_COMPLETEDDATE  := :new.completed_dt; 
        
    BEGIN
    ----- Refresh or Grass
        SELECT GROUP_NAME
        INTO   P_WORK_GROUP
        FROM   SQA_TD_WORK_GROUPS
        WHERE  WORKCODE = P_WORK_CODE;
        
          SELECT COUNT(*)
          INTO P_CNT
          FROM SQA_VENDOR_LIST
         WHERE  VENDOR_CODE  = P_CONTRACTOR
           AND   SEGMENTS    = P_REPORT_SEGMENT
           AND   WORKCODE    = P_WORK_GROUP;                
    EXCEPTION
         WHEN OTHERS THEN
         P_WORK_GROUP := 'ERROR';
         P_CNT        := -1;
    END;

         
  --- P_CNT 
  --    0  NEW CONTRACTOR 
  ---   1  CONTRACTOR LISTED
  ---- -1  BAD WORK CODE

/*

   This is a new contractor
   set the starting counting point to the max (trunc(completed date)) - 90
   from the workorder view
   where contractor = p_contractor

   the followup date comes from the 30/50/100 rule
   the followup is Initial

 */

   IF ( P_CNT = 0 )
      THEN

      START_CHECK_DATE := TRUNC(P_COMPLETEDDATE) - 1;

     begin

/*
           FOLLOW_UP_DTE         = NULL,
           NEXT_REVIEW           = NULL,
           FOLLOW_UP             = 'Initial',
           STANDING              = BOTTOM_OF_LIST,
           NBR_WORKORDERS        =  0,
           NBR_COMPLETED         =  0,
           THIRTY_FIFTY_DAY_RULE =  0,
           START_COUNTER_DATE    =  (GV_CURRENT_DATE + 1),
           ASSIGN_IT             =  0,
           COMPLETED_BY          =  0,
           ASSIGNED_TO           =  0,
           BATCH_NO              =  0,
           LAST_REVIEW           = GV_CURRENT_DATE

*/

                   INSERT INTO SQA_VENDOR_LIST(
                                  VENDOR_CODE,
                                  STANDING ,
                                  ACTIVE,
                                  SEGMENTS,
                                  WORKCODE,
                                  START_COUNTER_DATE,
                                  FOLLOW_UP,
                                  FOLLOW_UP_CAT,
                                  THIRTY_FIFTY_DAY_RULE,
                                  BATCH_NO,
                                  ASSIGNED_TO,
                                  ASSIGN_IT,
                                  COMPLETED_BY,
                                  NBR_COMPLETED,
                                  NBR_WORKORDERS)
                           VALUES (P_CONTRACTOR,
                                  (P_STANDING +1),
                                   1,
                                  P_REPORT_SEGMENT,
                                  P_WORK_GROUP,
                                  START_CHECK_DATE,
                                  'Initial',
                                  'INITIAL-NEW-TDA-VENDOR',
                                   0,
                                   0,
                                   0,
                                   0,
                                   0,
                                   0,
                                   0);
     exception
            WHEN OTHERS THEN
             NULL;
     END;


   END IF;



EXCEPTION
        WHEN BAD_DATA THEN
        SEND_EMAIL (P_TEAM=>'RDM',P_FROM=>'APEX SQA APP',P_SUBJECT=>'Error IN SQA_TD_DATA TRIGGER!!' ,P_MESSAGE=>'-'||P_CLIENT||'-'||P_WORK_CODE||'-'||P_LOAN_TYPE||'-' );

        WHEN OTHERS THEN
        MESSAGE := SQLERRM;

        SEND_EMAIL (P_TEAM=>'RDM',P_FROM=>'APEX SQA APP',P_SUBJECT=>'Error IN SQA_TD_DATA TRIGGER!!' ,P_MESSAGE=>MESSAGE );



END;
/
SHOW ERRORS


spool off;


