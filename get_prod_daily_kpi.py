import mysql.connector
import pandas as pd
import os

# DB CONFIG
conn = mysql.connector.connect(
    host="10.47.150.144",
    user="sqaviewjbr",
    password="5qAv13wJbr@2019#",
    database="kpi"
)
# QUERY
q_siteid = '''SELECT '2G', a.`tanggal`, a.`siteid`,b.branch,b.cluster,b.kabupaten,
SUM(a.tch_traffic_erl) traffic,SUM(a.gprs_payload_mbit+edge_payload_ul_mbit+a.edge_payload_dl_mbit)/8192 payload_GB
FROM `kpi`.`kpi_daily_2g` a
LEFT JOIN `test`.`dapot_sitename` b
ON a.`siteid` = b.`site_id`
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" AND LEFT(a.siteid,3) IN (
    'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB', 
    'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL', 
    'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX') '''+query_siteid+'''
GROUP BY a.`tanggal`, a.`siteid`

UNION

SELECT '3G', a.`tanggal`, a.`siteid`,b.branch,b.cluster,b.kabupaten,
SUM(a.traffic_voice_erlang+a.traffic_video_erlang) traffic,
SUM(a.ps_payload_dl_mbit+a.ps_payload_ul_mbit+a.hsdpa_payload_mbit+a.hsupa_payload_mbit)/8192 payload_GB
FROM `kpi`.`kpi_daily_3g` a
LEFT JOIN `test`.`dapot_sitename` b
ON a.`siteid` = b.`site_id`
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" AND LEFT(a.siteid,3) IN (
    'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB', 
    'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL', 
    'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX') '''+query_siteid+'''
GROUP BY a.`tanggal`, a.`siteid`

UNION

SELECT '4G', a.`tanggal`, a.`siteid`, b.branch,b.cluster,b.kabupaten,0,
SUM(a.`traffic_dl_volume_mbit`+a.`traffic_ul_volume_mbit`)/8192 payload_GB
FROM `kpi`.`kpi_daily_4g` a
LEFT JOIN `test`.`dapot_sitename` b
ON a.`siteid` = b.`site_id`
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" AND LEFT(a.siteid,3) IN (
    'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB', 
    'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL', 
    'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX') '''+query_siteid+'''
GROUP BY a.`tanggal`, a.`siteid`;
'''

name_file = 'prod_kpi_'+date_1+'_'+date_2+'.xlsx'

prod_siteid = pd.read_sql(q_siteid, conn)

with pd.ExcelWriter('''F:/KY/prodkpi/download/'''+name_file) as writer:
    prod_siteid.to_excel(
        writer, index=False, sheet_name='PROD_DAILY_SITEID')

update.message.bot.sendDocument(update.message.chat.id, open(
    'F:/KY/prodkpi/download/'+name_file, 'rb'))
