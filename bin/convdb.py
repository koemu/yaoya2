#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ----------------------------------------------
# makeServerInfo.py
#
# Copyright(C) 2013 HEARTBEATS Corporation.
# Author: Yuichiro SAITO <saito@heartbeats.jp>
# ----------------------------------------------

"""
CREATE TABLE `naya` (
  `group_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `host_name` varchar(45) NOT NULL,
  `command_name` varchar(255) NOT NULL,
  `execute_at` int(11) NOT NULL,
  `command_line` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `return_code` int(11) NOT NULL DEFAULT '0',
  `visible` int(1) NOT NULL DEFAULT '1',
  `output` longtext,
  `time` datetime DEFAULT NULL,
  PRIMARY KEY (`group_name`,`host_name`,`command_name`,`execute_at`),
  KEY `index1` (`visible`,`group_name`,`host_name`,`command_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 
"""

from pymongo import Connection, ASCENDING, DESCENDING
from elixir import *
import time
import logging
import sys
import dateutil
import datetime


#-----------------------------------------------

class NAYA( Entity ):
    using_options( tablename = "naya", autoload = True )


#-----------------------------------------------

def main():
    """
    Main
    """
    logging.basicConfig( level=logging.DEBUG )
    
    kvs_conn = Connection( "127.0.0.1", 27017 )
    coll  = kvs_conn[ "nouka" ][ "naya" ]
    metadata.bind = "mysql://nouka:nouka@127.0.0.1/nouka"
    metadata.bind.echo = False
    setup_all()

    hosts = coll.find()
    result = {}
    idx = 0
    for hostinfo in hosts:
        # データの有効性確認
        if hostinfo.get( "group_name", None ) is None \
            or hostinfo.get( "host_name", None ) is None \
            or hostinfo.get( "command_name", None ) is None \
            or hostinfo.get( "execute_at", None ) is None:
            logging.debug( hostinfo )
            continue
        # 重複確認
        if NAYA.query.filter_by(
            group_name   = hostinfo[ "group_name" ],
            host_name    = hostinfo[ "host_name" ],
            command_name = hostinfo[ "command_name" ],
            execute_at   = hostinfo[ "execute_at" ],
            ).first() is not None:
            continue
        
        # 文字コード置換・Insert
        if hostinfo.get( "command_line", None ) is not None:
            hostinfo[ "command_line" ] = hostinfo[ "command_line" ].encode('utf-8')
        if hostinfo.get( "output", None ) is not None:
            hostinfo[ "output" ] = hostinfo[ "output" ].encode('utf-8')
        try:
            record = NAYA(
                group_name    = hostinfo[ "group_name" ].encode('utf-8'),
                host_name     = hostinfo[ "host_name" ].encode('utf-8'),
                command_name  = hostinfo[ "command_name" ].encode('utf-8'),
                execute_at    = int( hostinfo[ "execute_at" ] ),
                command_line  = hostinfo.get( "command_line", None ),
                return_code   = int( hostinfo.get( "return_code", 0 ) ),
                visible       = int( bool( hostinfo.get( "visible", True ) ) ),
                output        = hostinfo.get( "output", None ),
                time          = hostinfo.get( "time", None ),
            )
        except:
            logging.error( sys.exc_info() )
            logging.debug( hostinfo )
            continue
        idx += 1
        if idx % 200 == 0:
            logging.debug( "Importing: %d" % idx )
            try:
                session.commit()
            except:
                session.rollback()
                logging.error( sys.exc_info() )
                logging.debug( hostinfo )
                break
    
    session.commit()
    session.close()
    
    return 0;


#-----------------------------------------------
if __name__ == '__main__':
    main()
