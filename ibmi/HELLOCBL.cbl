       IDENTIFICATION DIVISION.
       PROGRAM-ID. HELLOCBL.
       DATA DIVISION.
       LINKAGE SECTION.
       01 P-NAME   PIC X(20).
       01 P-GREET  PIC X(40).
       01 P-RC     PIC S9(9) COMP-4.
       PROCEDURE DIVISION USING P-NAME P-GREET P-RC.
       MAIN.
           MOVE SPACES TO P-GREET.
           IF P-NAME = SPACES
              MOVE 1 TO P-RC
              MOVE "NAME IS REQUIRED" TO P-GREET
           ELSE
              MOVE 0 TO P-RC
              STRING "HELLO " DELIMITED BY SIZE
                     P-NAME DELIMITED BY SPACE
                     " FROM COBOL" DELIMITED BY SIZE
                INTO P-GREET
           END-IF.
           GOBACK.
