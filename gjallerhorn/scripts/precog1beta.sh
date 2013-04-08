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
curl "https://beta.precog.com/analytics/v1/fs/0000000024/?apiKey=A1C62105-691B-4D77-9372-36A693E5D905&q=import%20std%3A%3Atime%3A%3A*%20data%20%3A%3D%20%2F%2Fnathan%2Fpoliticalsentiment%2Ftwitter%2F*%2Frawtweets%20combined%20%3A%3D%20solve%20'stateName%2C%20'state%20data'%20%3A%3D%20data%20where%20data.stateName%20%3D%20'stateName%20%26%20data.STATE%20%3D%20'state%20%7BstateName%3A%20'stateName%2C%20state%3A%20'state%2C%20obamaSentimentScore%3A%20mean(data'.score%20where%20data'.candidate%20%3D%20%22Obama%22)%2C%20romneySentimentScore%3A%20mean(data'.score%20where%20data'.candidate%20%3D%20%22Romney%22)%7D%20%7BstateName%3A%20combined.stateName%2C%20state%3A%20combined.state%2C%20sentiment%3A%20(50%20*%20(combined.obamaSentimentScore%20-%20combined.romneySentimentScore))%20%2B%2050%7D"

#Precog Query: Election 2012 Twitter GeoChart (all data -- 1.8 million rows) ran in 240 seconds on 3-22-13