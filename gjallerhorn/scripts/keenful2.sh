curl "http://staging.precog.com/analytics/v1/fs/0000000071?apiKey=BD491DD0-B3C3-4D3B-A5BC-ECDED9798CA8&q=import%20std%3A%3Atime%3A%3A*%20data%20%3A%3D%20%2F%2Fprod%2F510d6f801c8430000967f555%2Factions%2F%20range%20%3A%3D%20data%20where%20getMillis%20(data.action.created_date)%20%3E%20getMillis%20(%222013-03-03%22)%20%26%20getMillis%20(data.action.created_date)%20%3C%20getMillis%20(%222013-03-10%22)%20solve%20'day%20data'%20%3A%3D%20range%20where%20dateHour(range.action.created_date)%20%3D%20'day%20%7Bday%3A%20'day%2C%20views%3A%20count(data'.action.verb%20where%20data'.action.verb%20%3D%20%22view%22)%2C%20clicks%3A%20count(data'.action.verb%20where%20data'.action.verb%20%3D%20%22click%22)%2C%20recommendations%3A%20count(data'.action.verb%20where%20data'.action.verb%20%3D%20%22recommend%22)%2C%20total%3A%20count(data'.action.verb)%2C%20sets%3A%20count(distinct(data'.action.set_id%20where%20data'.action.verb%20%3D%20%22recommend%22))%20%7D"

#Keenful Query, ran in 23 seconds on 3-22-13