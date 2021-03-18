SELECT Employees.ename, Certified.eid, Schedule.aid
FROM Certified,Schedule,Employees
WHERE Certified.eid=Employees.eid,Certified.aid=Schedule.aid