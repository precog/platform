#!/bin/bash

(echo -e "<h1>FIXMEs</h1>\n\n<pre>";
 ack-term --nocolor -i --heading --break --scala 'FIXME';
 echo -e "\n</pre><h1>TODOs</h1>\n\n<pre>";
 ack-term --nocolor -i --heading --break --scala 'TODO(?!uble)';
 echo "</pre>") | mutt -e "set content_type=text/html" -s "FIXME/TODO report `date '+%F'`" engineering@precog.com
