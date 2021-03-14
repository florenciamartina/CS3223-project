#touch query.out
java RandomDB CUSTOMER 200
java ConvertTxtToTbl CUSTOMER
#java QueryMain query1.sql query.out
#Naive select without join

java RandomDB CART 200
java ConvertTxtToTbl CART

#java QueryMain query3.sql query.out
#Select 5 attributes and 1 join

#Need to write the code for the experiments

#Vanilla SQL join code
#
#Select C.eid
#From Certified C, Schedule S
#where C.aid = S.aid


#200 is quite fast