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
curl "http://staging.precog.com/analytics/v1/fs/0000000056/se8429501?apiKey=5D28D0F7-2678-4E1F-B30C-6B453A8BC8A8&q=import%20std%3A%3Astats%3A%3A*%20import%20std%3A%3Atime%3A%3A*%20agents%20%3A%3D%20%2F%2F8504352d-b063-400b-a10b-d6c637539469%2Fstatus%20upperBound%20%3A%3D%201353145306278%20lowerBound%20%3A%3D%201353135306278%20extraLB%20%3A%3D%20lowerBound%20-%20(24*60*60000)%20results%20%3A%3D%20solve%20'agent%20agents'%20%3A%3D%20agents%20where%20agents.timestamp%20%3C%3D%20upperBound%20%26%20agents.timestamp%20%3E%3D%20extraLB%20%26%20agents.agentId%20%3D%20'agent%20order%20%3A%3D%20denseRank(agents'.timestamp)%20agents''%20%3A%3D%20agents'%20with%20%7B%20rank%3A%20order%20%7D%20newagents%20%3A%3D%20new%20agents''%20newagents'%20%3A%3D%20newagents%20with%20%7B%20rank%3A%20newagents.rank%20-%201%20%7D%20result%20%3A%3D%20newagents'%20~%20agents''%20%7B%20first%3A%20agents''%2C%20second%3A%20newagents'%20%7D%20where%20newagents'.rank%20%3D%20agents''.rank%20%7Bstart%3A%20std%3A%3Amath%3A%3Amax(result.first.timestamp%2C%20lowerBound)%2C%20end%3A%20result.second.timestamp%2C%20agentId%3A%20result.first.agentId%2C%20status%3A%20result.first.status%2C%20name%3A%20result.first.agentAlias%2C%20data%3A%20result.first%7D%20results%20where%20results.end%20%3E%20lowerBound"

#SnapEngage Query, ran in 7 seconds on 3-22-13