SELECT SCHEDULE.aid,AIRCRAFTS.aid,AIRCRAFTS.aname
FROM SCHEDULE,AIRCRAFTS
WHERE SCHEDULE.aid=AIRCRAFTS.aid
ORDERBY SCHEDULE.aid,AIRCRAFTS.aname