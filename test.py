#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 10:47:43 2018

@author: daniel_usvyat
"""

from Athena_connect import Athena_connection 
import pandas as pd

ath_conn = Athena_connection()

sql = """
insert your test query
 ;
           """

df = pd.read_sql(sql,ath_conn.authenticate())
