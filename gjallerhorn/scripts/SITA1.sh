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
curl "http://staging.precog.com/analytics/v1/fs/0000000068?apiKey=9120C30A-DDDD-4E93-BA08-97C942645E7C&q=import%20std%3A%3Astats%3A%3A*%20locations%20%3A%3D%20%2F%2Fdevicelocations%2F2012%2F11%2F01%20points%20%3A%3D%20%7B%20x%3A%20locations.x%2C%20y%3A%20locations.y%20%7D%20model%20%3A%3D%20kMedians(points%2C%205)%20locations%20with%20%7Bclusters%3A%20assignClusters(points%2C%20model)%7D"

#SITA create clusters and assign clusters 400k rows ran in 25 seconds on 3-22-13