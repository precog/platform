## 
##  ____    ____    _____    ____    ___     ____ 
## |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
## | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
## |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
## |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
## 
## This program is free software: you can redistribute it and/or modify it under the terms of the 
## GNU Affero General Public License as published by the Free Software Foundation, either version 
## 3 of the License, or (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
## without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
## the GNU Affero General Public License for more details.
## 
## You should have received a copy of the GNU Affero General Public License along with this 
## program. If not, see <http://www.gnu.org/licenses/>.
## 
## 
curl "http://staging.precog.com/analytics/v1/fs/0000000071?apiKey=BD491DD0-B3C3-4D3B-A5BC-ECDED9798CA8&q=import%20std%3A%3Atime%3A%3A*%20data%20%3A%3D%20%2F%2Fprod%2F510d6f801c8430000967f555%2Factions%20data'%20%3A%3D%20data%20where%20getMillis%20(data.action.created_date)%20%3E%20getMillis(%222013-03-03%22)%20%26%20getMillis%20(data.action.created_date)%20%3C%20getMillis(%222013-03-10%22)%20count(distinct(data'.visitor.id))"

#Keenful Query, ran in 9 seconds on 3-22-13