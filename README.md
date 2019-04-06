# CCC Project 

## 关于目前的版本：
有个很严重的问题，
master node读取全部的数据，我这里根本没有用到分开的计算。 
按照大佬的说法
大概有两种思路：

第一种是 所有人都load， 然后然后所有人都一起一行行读 但是 只有行号 n 符合条件 n % size == rank 的时候再处理， size是node总数 rank是当前node的编号

第二种是 先按文件尺寸把文件分成n块
比如 如果文件是 100  有两个node 就是 node 0: 0~50 node 1: 51~100 
但是这个位置不是准确的按行...所以要先跳到这个位置 向后找最近的一个换行符，那里才是真正的开始点（不过要注意开头和结尾之类的细节...反正就是不能有覆盖的地方 也不能漏掉）



# Some information extrated from lms discussion board: 

A student asked me after the lecture if regular expressions were needed to deal with hashtags used in substrings. Short answer is no. I only want / need SPACE#<STRING>SPACE. A few examples to illustrate (the quotation marks indication string start/end in the JSON file).

" #melbourne " and " #Melbourne " are  the same (as discussed previously even though upper/lower case differences)

"xyz#Melbourne" and " #Melbourne " are not the same - there is no space before the # in the first example

" #Melbourne " and " #melbourne! " or variations thereof are not the same (in the second example, the ! comes before a space is reached)

Ok?

Twitter documentation doesn't really apply here as they use hashtags for other things, e.g. topic modelling. I only need exact strings to match for this assignment. Things would get too complicated otherwise

url: https://app.lms.unimelb.edu.au/webapps/discussionboard/do/message?action=list_messages&course_id=_382322_1&nav=discussion_board_entry&conf_id=_813239_1&forum_id=_458671_1&message_id=_1870935_1

# Notice the format we are looking at: 

 IT is SPACE #STRING SPACE 

 关于读取：
 https://stackoverflow.com/questions/6886283/how-i-can-i-lazily-read-multiple-json-values-from-a-file-stream-in-python/7795029#7795029

 https://stackoverflow.com/questions/21533894/how-to-read-line-delimited-json-from-large-file-line-by-line

 ## a Sample format of a smallTwitter json is: 
{"total_rows":3877777,"offset":805584,"rows":[
{"id":"570379215192727552","key":["r1r01cdn8nb4",2015,2,25],"value":{"type":"Feature","geometry":{"type":"Point","coordinates":[144.92340088,-37.95935781]},"properties":{"created_at":"Wed Feb 25 00:26:16 +0000 2015","text":"For the Oscars, Lady Gaga trained with a vocal coach DAILY for 6 months http://t.co/DIIS5EtsW9 #melbourne http://t.co/ZSu8FifNUK","location":"melbourne"}},"doc":{"_id":"570379215192727552","_rev":"1-fa6a485cb4fe0575781b6c29286af554","contributors":null,"truncated":false,"text":"For the Oscars, Lady Gaga trained with a vocal coach DAILY for 6 months http://t.co/DIIS5EtsW9 #melbourne http://t.co/ZSu8FifNUK","in_reply_to_status_id":null,"favorite_count":0,"source":"<a href=\"http://dlvr.it\" rel=\"nofollow\">dlvr.it</a>","retweeted":false,"coordinates":{"type":"Point","coordinates":[144.92340088,-37.95935781]},"entities":{"symbols":[],"user_mentions":[],"hashtags":[{"indices":[95,105],"text":"melbourne"}],"urls":[{"url":"http://t.co/DIIS5EtsW9","indices":[72,94],"expanded_url":"http://j.mp/1ag2Quk","display_url":"j.mp/1ag2Quk"}],"media":[{"expanded_url":"http://twitter.com/ynnmedianetwork/status/570379215192727552/photo/1","display_url":"pic.twitter.com/ZSu8FifNUK","url":"http://t.co/ZSu8FifNUK","media_url_https":"https://pbs.twimg.com/media/B-pk38nVEAAP9dj.jpg","id_str":"570379215142457344","sizes":{"large":{"h":380,"resize":"fit","w":380},"small":{"h":340,"resize":"fit","w":340},"medium":{"h":380,"resize":"fit","w":380},"thumb":{"h":150,"resize":"crop","w":150}},"indices":[106,128],"type":"photo","id":570379215142457340,"media_url":"http://pbs.twimg.com/media/B-pk38nVEAAP9dj.jpg"}]},"in_reply_to_screen_name":null,"in_reply_to_user_id":null,"retweet_count":0,"id_str":"570379215192727552","favorited":false,"user":{"follow_request_sent":false,"profile_use_background_image":true,"profile_text_color":"333333","default_profile_image":false,"id":2543131938,"profile_background_image_url_https":"https://abs.twimg.com/images/themes/theme1/bg.png","verified":false,"profile_location":null,"profile_image_url_https":"https://pbs.twimg.com/profile_images/567602629937606657/ZCcCDFzr_normal.jpeg","profile_sidebar_fill_color":"DDEEF6","entities":{"url":{"urls":[{"url":"http://t.co/lSobtgeAxq","indices":[0,22],"expanded_url":"http://youthsnews.com.au","display_url":"youthsnews.com.au"}]},"description":{"urls":[]}},"followers_count":68313,"profile_sidebar_border_color":"C0DEED","id_str":"2543131938","profile_background_color":"C0DEED","listed_count":6,"is_translation_enabled":false,"utc_offset":36000,"statuses_count":1390,"description":"media network","friends_count":788,"location":"pacific, oceania","profile_link_color":"042A38","profile_image_url":"http://pbs.twimg.com/profile_images/567602629937606657/ZCcCDFzr_normal.jpeg","following":false,"geo_enabled":true,"profile_banner_url":"https://pbs.twimg.com/profile_banners/2543131938/1424079798","profile_background_image_url":"http://abs.twimg.com/images/themes/theme1/bg.png","name":"ynnmedia™","lang":"en","profile_background_tile":false,"favourites_count":765,"screen_name":"ynnmedianetwork","notifications":false,"url":"http://t.co/lSobtgeAxq","created_at":"Tue Jun 03 09:27:23 +0000 2014","contributors_enabled":false,"time_zone":"Yakutsk","protected":false,"default_profile":false,"is_translator":false},"geo":{"type":"Point","coordinates":[-37.95935781,144.92340088]},"in_reply_to_user_id_str":null,"possibly_sensitive":false,"lang":"en","created_at":"Wed Feb 25 00:26:16 +0000 2015","in_reply_to_status_id_str":null,"place":null,"metadata":{"iso_language_code":"en","result_type":"recent"},"location":"melbourne"}},