#!/usr/bin/env python3
# vim:fenc=utf-8:noet
##  Copyright 2021 sysops.tv ;-)
##  BSD-2-Clause
##
##  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
## 
##  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
## 
##  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
## 
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
## THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
## BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
## GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
## LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

VERSION = 1.38

import sys
import re
import subprocess
import time
import json
import os.path
import socket
from email.message import EmailMessage

class zfscheck(object):
    ZFSLIST_REGEX = re.compile("^(?P<dataset>.*?)(?:|@(?P<snapshot>.*?))\t(?P<creation>\d+)\t(?P<guid>\d+)\t(?P<used>\d+)\t(?P<referenced>\d+)$",re.M) ## todo used/referenced ... diff zum replica
    ZFS_LOCAL_SNAPSHOTS = []
    ZFS_DATASTORES = {}
    ZFS_RESULT_SNAPSHOT = {}
    VALIDCOLUMNS = ["dataset","snapshot","creation","guid","used","referenced","size","age","status","copy"] ## valid columns
    DEFAULT_COLUMNS = ["dataset","snapshot","age","count","copy"] ## default columns
    DATEFORMAT = "%a %d.%b.%Y %H:%M"
    COLOR_CONSOLE = {
        "warn"  : "\033[93m",
        "crit"  : "\033[91m",
        "reset" : "\033[0m"
    }
    COLUMN_NAMES = {  ## Namen frei editierbar
        "dataset"   : "Dataset",
        "snapshot"  : "Snapshotname",
        "creation"  : "Erstellungszeit",
        "age"       : "Alter",
        "count"     : "Anzahl",
        "used"      : "Größe",
        "copy"      : "Replikat"
    }
    COLUMN_ALIGN = {
        "dataset"   : "<",
        "snapshot"  : "<",
        "copy"      : "<",
        "status"    : "^"
    }
    
    TIME_MULTIPLICATOR = { ## todo
        "h" : 60, ## Stunden 
        "d" : 60*24, ## Tage
        "w" : 60 * 24 * 7, ## Wochen
        "m" : 60 * 24 * 30 ## Monat
    }
    COLUMN_MAPPER = {}
    
    def __init__(self,**kwargs):
        _start_time = time.time()
        self.remote = None
        self.filter = None
        self.rawdata = False
        self.sortreverse = False
        self._check_kwargs(kwargs)
        _data = self.get_data()
        _script_runtime = time.time() - _start_time
        if self.output == "text":
            print(self.table_output(_data))
        if self.output == "html":
            print( self.html_output(_data) )
        if self.output == "mail":
            self.mail_output(_data)
        if self.output == "checkmk":
            self.checkmk_output(_data)
        if self.output == "json":
            print(self.json_output(_data))

        print ("Runtime: {0:.2f}".format(_script_runtime))

    def _check_kwargs(self,kwargs):
        ## argumente überprüfen
        for _k,_v in kwargs.items():
            if _k == "columns":
                _default = self.DEFAULT_COLUMNS[:]

                if not _v:
                    self.columns = _default
                    continue ## defaults
                # add modus wenn mit + 
                if not _v.startswith("+"):
                    _default = []
                else:
                    _v = _v[1:]
                _v = _v.split(",")
                for _column in _v:
                    if _column not in self.VALIDCOLUMNS:
                        raise Exception("invalid column {0} ({1})".format(_v,",".join(self.VALIDCOLUMNS)))
                    _default.append(_column)
                _v = list(_default)


            if _k == "sort" and _v:
                ## sortierung desc wenn mit +
                if _v.startswith("+"):
                    self.sortreverse = True
                    _v = _v[1:]
                if _v not in self.VALIDCOLUMNS:
                    raise Exception("invalid sort column: {0} ({1})".format(_v,",".join(self.VALIDCOLUMNS)))

            if _k == "threshold" and _v:
                _v = _v.split(",")
                ## todo tage etc
                _v = list(map(int,_v[:2])) ## convert zu int
                if len(_v) == 1:
                    _v = (float("inf"),_v[0])

            if _k == "filter" and _v:
                _v = re.compile(_v)

            setattr(self,_k,_v)
        
        ## funktionen zum anzeigen / muss hier da sonst kein self
        if not self.rawdata:
            self.COLUMN_MAPPER = {
                "creation"      : self.convert_ts_date,
                "age"           : self.seconds2timespan,
                "used"          : self.format_bytes,
                "size"          : self.format_bytes,
                "referenced"    : self.format_bytes,
            }

    def get_data(self):
        _data = self._call_proc()
        self.get_local_snapshots(_data)
        if self.remote:
            _data = self._call_proc(self.remote)
        
        return self.get_snapshot_results(_data)

    def get_local_snapshots(self,data):
        for _entry in self._parse(data):
            _entry.update({
                "creation"  : int(_entry.get("creation",0))
            })
            if _entry.get("snapshot") == None:
                self.ZFS_DATASTORES[_entry.get("dataset")] = _entry
            self.ZFS_LOCAL_SNAPSHOTS.append(_entry)

    def get_snapshot_results(self,data):
        _now = time.time()
        for _entry in self._parse(data):
            if _entry.get("snapshot") == None:
                continue ## TODO 
            if self.filter and not self.filter.search("{dataset}@{snapshot}".format(**_entry)):
                continue
            _timestamp = int(_entry.get("creation",0))
            _dataset = _entry["dataset"]
            _entry.update({
                "creation"  : _timestamp,
                "age"       : int(_now - _timestamp),
                "count"     : 1,
                "size"      : self.ZFS_DATASTORES.get(_dataset,{}).get("used",0),
                "used"      : int(_entry.get("used",0)),
                "referenced": int(_entry.get("referenced",0)),
                "copy"      : "",
                "status"    : self.check_threshold(_now -_timestamp)
            })
            _copys = list(
                filter(lambda x: x.get("guid") == _entry.get("guid") and x.get("dataset") != _entry.get("dataset") , self.ZFS_LOCAL_SNAPSHOTS)
            )
            if len(_copys) > 0:
                _entry["copy"] = ",".join(["{0}".format(_x.get("dataset")) for _x in _copys])
            else:
                if self.backup:
                    continue
            _exist_entry = self.ZFS_RESULT_SNAPSHOT.get(_dataset)
            if _exist_entry:
                _entry["count"] += _exist_entry.get("count") ## update counter
                if _exist_entry.get("creation") <= _entry.get("creation"): ## newer
                    _exist_entry.update(_entry)
                else:
                    _exist_entry["count"] = _entry["count"]
            else:
                self.ZFS_RESULT_SNAPSHOT[_dataset] = _entry
                    
        
        return list(self.ZFS_RESULT_SNAPSHOT.values())

    def _parse(self,data):
        _ret = [] ## Fixme
        for _match in self.ZFSLIST_REGEX.finditer(data):
            #yield _match.groupdict()
            _ret.append(_match.groupdict())
        return _ret

    def _call_proc(self,remote=None):
        zfs_args = ["zfs", "list", 
                "-t", "all",  ## list snapshots / TODO:all
                "-Hp",  ## script und numeric output
                "-o", "name,creation,guid,used,referenced",  ## attributes to show
                "-r" ## recursive
        ]
        if remote:
            _privkeyoption = []
            if self.ssh_identity:
                _privkeyoption = ["-i",self.ssh_identity]
            _sshoptions = "BatchMode yes"
            _parts = remote.split(":")
            _port = "22"  ## default port
            if len(_parts) > 1:
                remote = _parts[0]
                _port = _parts[1]
            zfs_args = ["ssh",
                remote, ## Hostname
                "-p", _port,
                "-o", _sshoptions, ## ssh options
            ] + _privkeyoption + zfs_args
        _proc = subprocess.Popen(zfs_args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False)
        _stdout, _stderr = _proc.communicate()
        if _proc.returncode > 0:
            raise Exception(_stderr.decode(sys.stdout.encoding)) ## Raise Errorlevel with Error from proc
        return _stdout.decode(sys.stdout.encoding)

    def convert_ts_date(self,ts):
        return time.strftime(self.DATEFORMAT,time.localtime(ts))

    def check_threshold(self,age):
        if not self.threshold:
            return "ok"
        age /= 60 ## default in minuten
        #print("Age: {0} - {1} - {2}".format(age,list(zip(self.threshold,("warn","crit"))),list(filter(lambda y: y[0] < age,zip(self.threshold,("warn","crit"))))))
        _status = list(
            map(lambda x: x[1], ## return only last
                filter(lambda y: y[0] < age, ## check threshold Texte
                    zip(self.threshold,("warn","crit"))
                )
            )
        )
        if not _status:
            _status = ["ok"]
        return _status[-1]
        

    @staticmethod
    def format_bytes(size,unit='B'):
        # 2**10 = 1024
        size = float(size)
        if size == 0:
            return "0"
        power = 2**10
        n = 0
        power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
        while size > power:
            size /= power
            n += 1
        return "{0:.2f} {1}{2}".format(size, power_labels[n],unit)

    @staticmethod
    def seconds2timespan(seconds,details=2,seperator=" ",template="{0:.0f}{1}",fixedview=False):
        _periods = (
            ('W', 604800),
            ('T', 86400),
            ('Std', 3600),
            ('Min', 60),
            ('Sek', 1)
        )
        _ret = []
        for _name, _period in _periods:
            _val = seconds//_period
            if _val:
                seconds -= _val * _period
                #if _val == 1:
                #    _name = _name[:-1]
                _ret.append(template.format(_val,_name))
            else:
                if fixedview:
                    _ret.append("")
        return seperator.join(_ret[:details])

    def _datasort(self,data):
        if not self.sort:
            return data
        return sorted(data, key=lambda k: k[self.sort],reverse=self.sortreverse)

    def checkmk_output(self,data):
        if not data:
            return
        print ("<<<checkzfs>>>")
        for _item in data:
            print("<<{0}>>".format(_item.get("dataset","").replace(" ","_")))
            for _info,_val in _item.items():
                print("{0}: {1}".format(_info,_val))

    def table_output(self,data):
        if not data:
            return
        _header = data[0].keys() if not self.columns else self.columns
        _header_names = [self.COLUMN_NAMES.get(i,i) for i in _header]
        _converter = dict((i,self.COLUMN_MAPPER.get(i,(lambda x: str(x)))) for i in _header)
        
        _output_data = [_header_names]
        _line_status = []
        for _item in self._datasort(data):
            _line_status.append(_item.get("status"))
            _output_data.append([_converter.get(_col)(_item.get(_col,"")) for _col in _header])

        _maxwidth = [max(map(len,_col)) for _col in zip(*_output_data)] ## max column breite
        _format = " ║ ".join(["{{:{}{}}}".format(self.COLUMN_ALIGN.get(_h,">"),_w) for _h,_w in zip(_header,_maxwidth)])  ## format bilden
        _line_print = False
        _out = []
        _status = "ok"
        for _item in _output_data:
            if _line_print:
                _status = _line_status.pop(0)
            if _status != "ok":
                _out.append(self.COLOR_CONSOLE.get(_status,"") + _format.format(*_item) + self.COLOR_CONSOLE.get("reset"))
            else:
                _out.append(_format.format(*_item))
            if not _line_print:
                _out.append("═╬═".join(map(lambda x: x*"═",_maxwidth))) ## trennlinie
                _line_print = True  
        return "\n".join(_out)

    def html_output(self,data):
        if not data:
            return
        _header = data[0].keys() if not self.columns else self.columns
        _header_names = [self.COLUMN_NAMES.get(i,i) for i in _header]
        _converter = dict((i,self.COLUMN_MAPPER.get(i,(lambda x: str(x)))) for i in _header)
        
        _out = []
        _out.append("<html><head>")
        _out.append("<style type='text/css'>.warn { background-color: yellow } .crit { background-color: red}</style>")
        _out.append("<title>ZFS</title></head>")
        _out.append("<body><table border=1>")
        _out.append("<tr><th>{0}</th></tr>".format("</th><th>".join(_header_names)))
        for _item in self._datasort(data):
            _out.append("<tr class='{1}'><td>{0}</td></tr>".format("</td><td>".join([_converter.get(_col)(_item.get(_col,"")) for _col in _header]),_item.get("status","ok")))
        _out.append("</table></body></html>")
        return "".join(_out)

    def mail_output(self,data):
        _msg = EmailMessage()
        self.hostname = socket.getfqdn()
        _msg.set_content(self.checkmk_output)
        _msg.add_alternative(self.html_output(data),subtype="html")
        _msg["From"] = "ZFS-Checkscript on {0} <root@{0}".format(self.hostname)
        _msg["To"] = "root@{0}".format(self.hostname)
        _msg["Content-Type"] 
        _msg["Subject"] = "ZFS-Check {0}".format(self.hostname.split(".")[0])
        subprocess.run(["/usr/sbin/sendmail","-t","-oi"], input=_msg.as_bytes())

    def json_output(self,data):
        return json.dumps(data)

if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser("Tool to check ZFS Replication age\n##########################################\n")
    _parser.add_argument('--remote',type=str,
                help="SSH Connection Data user@host")
    _parser.add_argument("--filter",type=str,
                help="Regex Filter only Snapshots")
    _parser.add_argument("--pool",type=str,
                help="Regex Filter only Pool")
    _parser.add_argument("--output",type=str,default="text",choices=["html","text","mail","checkmk","json"],
                help="Ausgabeformat")
    _parser.add_argument("--columns",type=str,
                help="Show only Columns")
    _parser.add_argument("--sort",type=str,choices=zfscheck.VALIDCOLUMNS,
                help="Sort by Column")
    _parser.add_argument("--threshold",type=str,
                help="Add Warn/Crit times in minutes")
    _parser.add_argument("--backup",action="store_true",
                help="search backups only")
    _parser.add_argument("--rawdata",action="store_true",
               help="show times/bytes unconverted")
    _parser.add_argument("--ssh-identity",type=str,
                help="Path to ssh private key to use")
    _parser.add_argument("--ssh-extra-option",type=str,
                help="additional ssh -o ... options")
    args = _parser.parse_args()
    zfscheck(**args.__dict__)

