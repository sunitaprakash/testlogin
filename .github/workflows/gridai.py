#!/usr/bin/env python3

import fire
import subprocess
import shlex
import os
from dataclasses import dataclass
from typing import List, Optional, final
from enum import Enum
import re
import logging
import time
import io
import json
import pandas as pd

def midtext(text:str,begin_text:str = None, begin_find_count:int = 1, end_text:str = None):
  """return text between begin_text and optional end_text markers"""
  start_pos=0
  for n in range(begin_find_count):
    new_pos = text[start_pos:].find(begin_text)
    print(new_pos)
    if new_pos > 0:
      start_pos = start_pos + new_pos + len(begin_text)
    else:
      return("")   
  print(f"start={start_pos}")
  end_pos = None # no end marker
  if end_text is not None:
    end_pos = text[start_pos:].find(end_text)
    if end_pos < 0:
      end_pos = 0 # don't return 
  return text[start_pos:end_pos]

def text_to_csv(lines, csvs=[], head:int = None, delimiter:str= '│|┃|\|'):
  """convert grid cli output to csv format"""
  csv = []
  n=0
  last_field_count = None
  for l in lines:
    c = [x.strip() for x in re.split(delimiter, l)]
    c = c[1:-1] # remove empty columns before the first and the last delim
    current_field_count = len(c)
    if current_field_count > 1:
      if last_field_count is None:
        last_field_count = current_field_count
      elif current_field_count != last_field_count:
        last_field_count = current_field_count   
        logging.debug(f"new table found")
        csvs.append(csv)
        n=0
        csv = []
      csv.append(c)
      n += 1
      if head is not None and n >= head:
        break
  csvs.append(csv)
  return csvs

def text_to_kv(lines, kvs=[], delimiter:str='\:', grep:str=r'^\s+'):
  """convert grid cli output to k:v format"""
  for l in lines:
    if re.match(grep, l):
      c = [x.strip() for x in re.split(":", l, maxsplit=2)]
      current_field_count = len(c)
      if current_field_count > 1:
        kvs.append(c)

  # convert to lower case for consistency
  for kv in kvs:
    kv[0] = kv[0].replace(' ', '_').lower()      
  return kvs

def kv_to_dict(kvs, result={}):
  """convert k:v format to dict format"""
  for kv in kvs:
    suffix_str=""
    suffix_numeric=1
    # add suffix to dups
    k=f"{kv[0]}{suffix_str}"
    while k in result:
      suffix_numeric += 1
      suffix_str = str(suffix_numeric)
      k=f"{kv[0]}{suffix_str}"
      logging.info(f"converting {kv[0]} to {k}")
    result[k] = kv[1]
  return result 
  
@dataclass
class StatusResult:
    """Grid.ai status search result"""
    # search
    type: str       # run|session|clusters|datastore
    id_col: str     # column name of id
    status_col: str # column name of status
    # df results
    f1: pd.DataFrame  # filter on id
    f2: pd.DataFrame  # filter on desired state + stopped state
    f3: pd.DataFrame  # filter on desired state
    # df length
    f1_len: int       # length of f1
    f2_len: int       # length of f2
    f3_len: int       # length of f3
    # lb <= f3 <= ub
    lb_check: bool    
    ub_check: bool

@dataclass
class CreateResult:
  """Grid.ai run,session,datastore,cluster create result"""
  # create
  type: str       # run|session|clusters|datastore
  # dict version of the output
  result: dict
  name:str 

class GridRetry(object):
  """Run Grid.ai commands and poll till successfully executed.
  NOTE: Put a leading space if argument has `--`. For example, `--grid_args " --use_spot"`. 
  Typical usage example:
    User examples:
      gridai.py cli "grid user" 
    Run examples:
      gridai.py create_login --grid_args " --username xxx --key xxx"
      gridai.py status_run imaginary-chaum-3228
      gridai.py status_run imaginary-chaum-3228 --status3 "failed" --poll_interval_sec 1
      gridai.py status_run cocky-cori-9304
      gridai.py status_run cocky-cori-9304 --status3 "failed" --poll_interval_sec 1
      gridai.py create_run hello.py --grid_args " --use_spot" --script_args " --number 1"      --gha True
      gridai.py create_run hello.py --grid_args " --use_spot" --script_args " --number [1,2]"  --gha True
      name=r$(date '+%y%m%d-%H%M%S'); gridai.py --name $name create_run hello.py --grid_args " --use_spot" --script_args " --number 1" --gha True
      gridai.py create_run hello.py --grid_args " --use_spot" --script_args " --number 1" --cluster $cluster_name
    Session examples:
      gridai.py create_sess --gha True
      name=s$(date '+%y%m%d-%H%M%S'); gridai.py create_sess --name $name --gha True
      name=s$(date '+%y%m%d-%H%M%S'); gridai.py create_sess --name $name --grid_args " --use_spot" --gha True
      name=s$(date '+%y%m%d-%H%M%S'); gridai.py create_sess --name $name --grid_args " --use_spot" --cluster $cluster_name
    Datastore examples:
      name=d$(date '+%y%m%d-%H%M%S'); gridai.py dat_create_poll --grid_args " --source . " --name "d${name}" --gha True
      name=d$(date '+%y%m%d-%H%M%S'); gridai.py dat_create_poll --grid_args " --source . " --name "d${name}" --cluster $cluster_name
    Cluster examples:
      export cluster_name=c$(date '+%y%m%d-%H%M%S')
      gridai.py clu_create_poll --name "$cluster_name" --grid_args "aws --role-arn $ROLE_ARN --external-id $EXTERNAL_ID --region us-east-1 --cost-savings --instance-types t2.medium,g4dn.xlarge"
      gridai.py cli "grid cluster delete $cluster_name" 
      grid delete cluster c211126-095228 cluster c211126-095228 
  """
  # stdout and stderr check
  # terminate (don't retry) on these messages
  terminate_messages = [
    "Error: Not authorized.",
    "Error: Invalid value for 'SCRIPT':",
    "Error: No such option:"
  ]
  # categorized comm error
  communication_messages = [
    "Error: Grid is unreachable.",
  ]
  exceed_time_cnt = 0
  cmd_errs_cnt = 0
  comm_errs_cnt = 0
  total_retry_cnt = 0
  search_params = []

  # transformation pipeline 
  po = None   # cli raw outout
  sr = None   # the last search results
  cr = None   # k:v to json create result
  
  # search csv

  def __init__(self,
    cwd:str=os.getcwd(), log_level=logging.INFO, cmd_exec_timeout:int=120, max_term_cols:int=512, poll_interval_sec:int=60, 
    max_total_retry_cnt:int=60, max_comm_errs_cnt:int=10, max_cmd_errs_cnt:int=3, max_exceed_time_cnt:int=3,
    github_actions:bool=True,chain:bool = False, gha:bool = False,
    # used in cli commands
    name:str = "", cluster:str = "", grid_args:str = "", script_args:str = "", ignore_warnings_arg:str = "--ignore_warnings",
    # used in status
    max_no_ids_cnt:int=3, max_no_match_cnt:int=0, max_some_match_cnt:int=3, max_state_flip_cnt:int=3,min_all_match_cnt:int=1,
    ):

    self.cwd = cwd 
    self.max_term_cols = max_term_cols 
    self.cmd_exec_timeout = cmd_exec_timeout 
    self.poll_interval_sec = poll_interval_sec 
    self.max_total_retry_cnt = max_total_retry_cnt
    self.max_exceed_time_cnt = max_exceed_time_cnt 
    self.max_comm_errs_cnt = max_comm_errs_cnt 
    self.max_cmd_errs_cnt = max_cmd_errs_cnt 
    self.github_actions = github_actions 
    self.log_level = log_level 
    self.chain = chain
    self.gha = gha

    # used in grid commands
    self.ignore_warnings_arg = ignore_warnings_arg 
    self.grid_args = grid_args
    self.script_args = script_args
    if (name == ""):
      self.name = ""
      self.name_arg = ""
    else:
      self.name = name
      self.name_arg = f"--name {name}"

    if (cluster == ""):
      self.cluster_name = ""
      self.cluster_arg = ""
    else:
      self.cluster_name = cluster
      self.cluster_arg = f"--cluster {cluster}"

    # used in timeout controls
    if (min_all_match_cnt < 1): raise ValueError(f"min_all_match_cnt={min_all_match_cnt} must be greater than 0")
    self.max_no_ids_cnt = max_no_ids_cnt 
    self.max_no_match_cnt = max_no_match_cnt 
    self.max_some_match_cnt = max_some_match_cnt 
    self.min_all_match_cnt = min_all_match_cnt 
    self.max_state_flip_cnt = max_state_flip_cnt 

    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s', level=log_level)

  def __str__(self):
    outputs=[]
    # simple CLI was run - show the output of the command
    if (self.sr is None and self.cr is None):
      return (self.po.stdout.decode("utf-8")+"\n"+self.po.stderr.decode("utf-8"))
    # more than a command was run
    else:
      if self.gha == True:
        # return the last status code
        outputs.append(f"::set-output name=obj-exit-code::{self.po.returncode}")
        if self.sr is not None:
          outputs.append(f"::set-output name=obj-type::{self.sr.type}")
          outputs.append(f"::set-output name=match-status::{','.join(self.sr.f3[self.sr.status_col])}")
          outputs.append(f"::set-output name=match-names::{','.join(self.sr.f3[self.sr.id_col])}")
          outputs.append(f"::set-output name=filter1-cnt::{self.sr.f1_len}")
          outputs.append(f"::set-output name=filter2-cnt::{self.sr.f2_len}")
          outputs.append(f"::set-output name=filter3-cnt::{self.sr.f3_len}")
        if self.cr is not None:
          if self.cr.name is not None:
            outputs.append(f"::set-output name=obj-name::{self.cr.result[self.cr.name]}")    
          for k in self.cr.result :
            outputs.append(f"::set-output name={k}::{self.cr.result[k]}")    
        return "\n".join(outputs)
      else:
        return json.dumps([self.sr,self.cr])

  def __grid_user_etl(self,kvs):
    """convert teams grid user output to standard format"""
    try:
      if kvs[4][0] == 'teams':
        print(f"found teams")
        team,role = kvs[5][0].split("-")
        team = team.strip("_")
        role = role.strip("_")
        print(f"{team},{role}")
        if role == "role":
          kvs[4][1] = team
          kvs[5][0] = "role"
    except:
      pass
    return(kvs)      

  def cli(self, cmd:str):
    """run Grid.ai CLI command"""
    # shell is required to set the COLUMNS
    args = f"export COLUMNS={self.max_term_cols}; {cmd}"
    logging.info(args)
    # po might not be defined on abend
    while self.total_retry_cnt < self.max_total_retry_cnt:
      self.total_retry_cnt += 1
      try:
        self.po = subprocess.run(args, cwd=self.cwd, capture_output=True, shell=True, timeout=self.cmd_exec_timeout) 
        if self.po.returncode == 0:
          break

        terminate_rc = False
        for line in self.terminate_messages:
          if self.po.stdout.decode('utf-8').find(line) >= 0:
            terminate_rc = True
          if self.po.stderr.decode('utf-8').find(line) >= 0:
            terminate_rc = True

        communication_rc = False
        for line in self.communication_messages:
          if self.po.stdout.decode('utf-8').find(line) >= 0:
            communication_rc = True
          if self.po.stderr.decode('utf-8').find(line) >= 0:
            communication_rc = True

        if terminate_rc == True:
          logging.info(f"stderr:\n{self.po.stderr.decode('utf-8')}")
          logging.info(f"stdout:\n{self.po.stdout.decode('utf-8')}")
          break 
        elif communication_rc == True:
          self.comm_errs_cnt += 1
          logging.info(f"{args}:total_retry_cnt {self.total_retry_cnt}/{self.max_total_retry_cnt}:comm_errs_cnt {self.comm_errs_cnt}/{self.max_comm_errs_cnt} exited with {self.po.returncode}")
          logging.info(f"stderr:\n{self.po.stderr.decode('utf-8')}")
          logging.info(f"stdout:\n{self.po.stdout.decode('utf-8')}")
          if self.comm_errs_cnt >= self.max_comm_errs_cnt: 
            break
        else:
          self.cmd_errs_cnt += 1
          logging.info(f"{args}:total_retry_cnt {self.total_retry_cnt}/{self.max_total_retry_cnt}:cmd_errs_cnt {self.cmd_errs_cnt}/{self.max_cmd_errs_cnt} exited with {self.po.returncode}")
          logging.info(f"stderr:\n{self.po.stderr.decode('utf-8')}")
          logging.info(f"stdout:\n{self.po.stdout.decode('utf-8')}")
          if self.cmd_errs_cnt >= self.max_cmd_errs_cnt: 
            break
      except subprocess.TimeoutExpired:
        self.exceed_time_cnt += 1
        logging.info(f"{args}:total_retry_cnt {self.total_retry_cnt}/{self.max_total_retry_cnt}:cmd_errs_cnt {self.exceed_time_cnt}/{self.max_exceed_time_cnt} exceeded timeout={self.cmd_exec_timeout}s")
        if self.exceed_time_cnt >= self.max_exceed_time_cnt: 
          break
      # wait before running the command again
      time.sleep(self.poll_interval_sec)
    return self
       
  def status_summary(self,cmd:str, id:str,status2,status3, filter1,filter2,filter3,  lb,ub, status_col, id_col, type:str):
    logging.info(f"cmd={cmd} id={id} s2={status2} s3={status3} {filter1},{filter2},{filter3}")
    # counters
    cmd_no_ids_cnt=0 
    cmd_no_match_cnt=0 
    cmd_some_match_cnt=0 
    cmd_all_match_cnt=0 
    cmd_state_flip_cnt=0
    # loop until the end
    rc=1
    while self.total_retry_cnt < self.max_total_retry_cnt:
      self.cli(cmd)
      # retried did not yield any results
      if self.po is None: 
        break

      # scrape tabular output to dataframe
      csvs=text_to_csv(self.po.stdout.decode('utf-8').splitlines(),csvs=[])   # grab the first array from potential multiple tabular outputs
      csv=csvs[0]
      df = pd.DataFrame(csv[1:],columns=csv[0])                       # row 0 = column names, the rest = data
      # run the queries
      f1 = df.query(filter1)  
      f2 = f1.query(filter2)    
      f3 = f2.query(filter3)    

      f1_len = len(f1)
      f2_len = len(f2)
      f3_len = len(f3)

      # cardinality of the f2 check
      lb_check = True
      if lb is not None:
        lb_check = lb <= f3_len
      ub_check = True
      # ub is not set, then f1 and f2 counts have to match
      if ub is None:
        ub_check=f3_len == f1_len
      else:  
        ub_check=f3_len <= ub

      self.sr = StatusResult(type,id_col, status_col, f1,f2,f3,f1_len,f2_len,f3_len,lb_check, ub_check)

      tally=f1.groupby([status_col])[status_col].count()

      # match desired condition
      if (lb_check & ub_check):
        logging.info(f"f1={f1_len}:f2={f2_len}:f3={f3_len}:{lb}<={f2_len}<={ub}: matched {str(tally)}")
        cmd_all_match_cnt += 1
        if ( cmd_all_match_cnt >= self.min_all_match_cnt ):
          rc=0        
          break        
      # id not found
      elif (f1_len == 0):
        logging.info(f"f1={f1_len}:f2={f2_len}:f3={f3_len}:{lb}<={f2_len}<={ub}:{cmd_no_ids_cnt}/{self.max_no_ids_cnt} F1=0 {str(tally)}")
        cmd_no_ids_cnt += 1
        if (self.max_no_ids_cnt > 0 and cmd_no_ids_cnt >= self.max_no_ids_cnt ):
          break   
      # status not found
      elif (f2_len == 0 ):
        logging.info(f"f1={f1_len}:f2={f2_len}:f3={f3_len}:{lb}<={f2_len}<={ub}:{cmd_no_match_cnt}/{self.max_no_match_cnt} F2=0 {str(tally)}")
        cmd_no_match_cnt += 1
        if ( self.max_no_match_cnt > 0 and cmd_no_match_cnt >= self.max_no_match_cnt ):
          break  
      # not enough status match found
      else:
        logging.info(f"f1={f1_len}:f2={f2_len}:f3={f3_len}:{lb}<={f2_len}<={ub}:{cmd_some_match_cnt}/{self.max_some_match_cnt} more matches needed {str(tally)}")
        cmd_some_match_cnt += 1
        if ( self.max_some_match_cnt > 0 and cmd_some_match_cnt >= self.max_some_match_cnt ):
          break

      # continue loop    
      self.total_retry_cnt += 1
      time.sleep(self.poll_interval_sec)

    # show the last output
    logging.info(self.po.stdout.decode("utf-8"))

  def create_login(self):
    """grid login"""
    self.cli(f"grid login {self.cluster_arg} {self.grid_args}")
    if self.po.returncode == 0:
      self.cli(f"grid user")
      if self.po.returncode == 0:
        kvs = text_to_kv(self.po.stdout.decode('utf-8').splitlines(),grep=r'.*')
        kvs = self.__grid_user_etl(kvs)
        self.cr = CreateResult("Session", kv_to_dict(kvs), "username" )      
    return(self)  

  def create_run(self,script_name:str):
    """grid run, poll grid status, grid artifacts"""
    self.cli(f"grid run {self.cluster_arg} {self.name_arg} {self.ignore_warnings_arg} {self.grid_args} {script_name} {self.script_args}")
    if self.po.returncode == 0:
      self.cr = CreateResult("Run", kv_to_dict( text_to_kv(self.po.stdout.decode('utf-8').splitlines())), "grid_name" )      
      self.status_run(f"{self.cr.result['grid_name']}")
      if self.sr.f3_len > 0: # download if at least 1 exp succeeded
        self.cli(f"grid artifacts {self.cr.result['grid_name']}")
    return(self)  

  def create_sess(self):
    """grid session create, poll grid session, grid session ssh 'exit'"""
    self.cli(f"grid session create {self.cluster_arg} {self.name_arg} {self.grid_args}")
    if self.po.returncode == 0:
      self.cr = CreateResult("Session", kv_to_dict( text_to_kv(self.po.stdout.decode('utf-8').splitlines())), "name" )      
      self.status_sess(f"^{self.cr.result['name']}$")
      if self.sr.f3_len > 0: # set the ssh key
        self.cli(f"grid session ssh {self.cr.result['name']} 'exit'")
    return(self)  

  def dat_create_poll(self):
    """grid datastore create, poll grid datastore"""
    if self.name != "":
      self.cli(f"grid datastore create {self.cluster_arg} {self.name_arg} {self.grid_args}")
      self.cr = CreateResult("Datastore", {"name":self.name}, "name" )      
      if self.po.returncode == 0:
        self.status_data(self.name)
        if self.sr.f3_len > 0: 
          pass
      return(self)  
    else:    
      print("Error: --name is required") 

  def clu_create_poll(self):
    """grid clusters aws, poll grid clusters"""
    if self.name != "":
      self.cli(f"grid clusters {self.cluster_arg} {self.grid_args} {self.name}")
      self.cr = CreateResult("Datastore", {"name":self.name}, "name" )      
      if self.po.returncode == 0:
        self.status_clus(self.name)
        if self.sr.f3_len > 0: 
          pass
      return(self)  
    else:    
      print("Error: --name is required") 

  def status_grid(self, id:str, type="grid",  
  filter1:str = "Status.str.contains(@status1) & Run.str.contains(@id)", status1:str='queued', 
  filter2:str = "Status.str.contains(@status2) & Run.str.contains(@id)", status2:str='queued', 
  lb=1,ub=None, # desired cardinality of lower and upper bound 
  status="Status" 
  ):
    """ run grid status, poll until desired state is reached"""
    self.status_summary(f"grid status", "grid", id=id, filter1=filter1,filter2=filter2,status1=status1,status2=status2,lb=lb,ub=ub, status=status)

  def status_run(self, 
  id:str,
  status2:str='succeeded|cancelled|failed|stopped',
  status3:str='succeeded',
  filter1:str = "Experiment.str.contains(@id)",  
  filter2:str = "Status.str.contains(@status2)", 
  filter3:str = "Status.str.contains(@status3)",  
  lb=1,ub=None,  # desired cardinality of lower and upper bound 
  status_col="Status",
  id_col="Experiment",
  type="run", 
  ):
    """ run grid session, poll until desired state is reached"""
    self.status_summary(f"grid status {id}", id=id, filter1=filter1,filter2=filter2,filter3=filter3,status2=status2,status3=status3,lb=lb,ub=ub, status_col=status_col, id_col=id_col, type=type)

  def status_data(self, 
  id:str,
  status2:str='running|failed|stopped|pause',
  status3:str='running',
  filter1:str = "Name.str.contains(@id)",  
  filter2:str = "Status.str.contains(@status2)", 
  filter3:str = "Status.str.contains(@status3)",  
  lb=1,ub=None,  # desired cardinality of lower and upper bound 
  status_col="Status",
  id_col="Name",
  type="datastore", 
  ):
    """ run grid session, poll until desired state is reached"""
    self.status_summary(f"grid datastore", id=id, filter1=filter1,filter2=filter2,filter3=filter3,status2=status2,status3=status3,lb=lb,ub=ub, status_col=status_col, id_col=id_col, type=type)

  def status_sess(self, 
  id:str,
  status2:str='running|failed|stopped|pause',
  status3:str='running',
  filter1:str = "Session.str.contains(@id)",  
  filter2:str = "Status.str.contains(@status2)", 
  filter3:str = "Status.str.contains(@status3)",  
  lb=1,ub=None,  # desired cardinality of lower and upper bound 
  status_col="Status",
  id_col="Session",
  type="session", 
  ):
    """ run grid session, poll until desired state is reached"""
    self.status_summary(f"grid session", id=id, filter1=filter1,filter2=filter2,filter3=filter3,status2=status2,status3=status3,lb=lb,ub=ub, status_col=status_col, id_col=id_col, type=type)

  def status_clus(self, 
  id:str,
  status2:str='running|failed',
  status3:str='running',
  filter1:str = "id.str.contains(@id)",  
  filter2:str = "status.str.contains(@status2)", 
  filter3:str = "status.str.contains(@status3)",  
  lb=1,ub=None,  # desired cardinality of lower and upper bound 
  status_col="status",
  id_col="id",
  type="clusters", 
  ):
    """ run grid session, poll until desired state is reached"""
    self.status_summary(f"grid clusters", id=id, filter1=filter1,filter2=filter2,filter3=filter3,status2=status2,status3=status3,lb=lb,ub=ub, status_col=status_col, id_col=id_col, type=type)

if __name__ == '__main__':
  fire.Fire(GridRetry)
