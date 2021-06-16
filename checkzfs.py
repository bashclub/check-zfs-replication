#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8:noet
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

### for check_mk usage link or copy binary to check_mk_agent/local/checkzfs
### create /etc/check_mk/checkzfs ## the config file name matches the filename in check_mk_agent/local/
###         to create a diffent set, link script to check_mk_agent/local/checkzfs2 and create /etc/check_mk/checkzfs2
###
### source: host1                   # [optional] comma seperated hostnames to check for source
### remote: host1                   # [optional]     "              "           "
### prefix: host1                   # [optional] Prefix for check_mk Servicename - default REPLICA
### filter: rpool/data|replica      # [optional] regex filter to match source
### snapshotfilter:                 # [optional] regex filter to match snapshot name
### threshold: 20,40                # [optional] threshods warn,crit in minutes
### ssh-extra-options:              # [optional] comma seperated ssh options like added with -o 
### ssh-identity: /path/to/priv.key # [optional] path to ssh private key
### disabled: 1                     # [optional] disable the script with this config 
###

VERSION = 2.7

from pprint import pprint
import sys
import re
import subprocess
import time
import json
import os.path
import os
import socket
from email.message import EmailMessage
from email.mime.application import MIMEApplication
from email.utils import formatdate

_ = lambda x: x   ## inline translate ... maybe later

class zfs_snapshot(object):
    def __init__(self,dataset_obj,snapshot,creation,guid,written,**kwargs):
        self.replica = set()
        self.dataset_obj = dataset_obj
        self.snapshot = snapshot
        self.creation = int(creation)
        self.age = int(time.time() - self.creation)
        self.written = int(written)
        self.guid = guid

    def add_replica(self,snapshot):
            self.replica.add(snapshot)
            self.dataset_obj.add_replica(snapshot.dataset_obj)
            snapshot.dataset_obj.add_replica(self.dataset_obj)

    def __repr__(self):
        return f"{self.guid} {self.snapshot}\n"

    def __str__(self):
        return f"{self.guid} {self.snapshot}\n"



class zfs_dataset(object):
    def __init__(self,dataset,guid,used,available,creation,type,autosnapshot,checkzfs,remote=None,source=None,**kwargs):
        self.checkzfs = checkzfs not in ("false","ignore")
        self.snapshots = {}
        self.remote = remote
        self.source = source
        self.guid = guid
        self.dataset = dataset
        self.creation = creation = int(creation)
        self.autosnapshot = {"true":2,"false":0}.get(autosnapshot,1) ### macht für crit/warn/ok am meisten sinn so
        self.type = type
        self.used = int(used)
        self.available = int(available)
        self.replica = set()
        self.lastsnapshot = ""

    def add_snapshot(self,**kwargs):
        #print ("Add snapshot"+repr(kwargs))
        _obj = zfs_snapshot(self,**kwargs)
        self.snapshots[_obj.guid] = _obj
        return _obj

    def add_replica(self,ds_object,**kwargs):
        if self.autosnapshot > 0: ## wenn autosnap auf false wird das wohl das target sein
            self.replica.add(ds_object)

    def _get_latest_snapshot(self,source=None):
        _snapshots = self.sorted_snapshots()
        if source:
            _snapshots = list(filter(lambda x: x.guid in source.snapshots.keys(),_snapshots))
        return _snapshots[0] if _snapshots else None

    def sorted_snapshots(self):
        return sorted(self.snapshots.values(), key=lambda x: x.age)


    @property
    def dataset_name(self):
        if self.source:
            return f"{self.source}#{self.dataset}"
        if not self.remote:
            return self.dataset
        return f"{self.remote}#{self.dataset}"

    @property
    def latest_snapshot(self):
        if self.snapshots:
            return self.sorted_snapshots()[0]


    def get_info(self,source,threshold=None):
        _latest = self._get_latest_snapshot(source if source != self else None)
        _status = None
        _message = ""
        if source == self:
            if not self.replica:
                _status = 1 ## warn
                _message = _("kein Replikat gefunden")
        else:
            if self.autosnapshot == 1:
                #_status = 1 ## warn
                _message = _("com.sun:auto-snapshot ist nicht gesetzt")
            elif self.autosnapshot == 2:
                #_status = 2 ## crit
                _message = _("com.sun:auto-snapshot ist auf Replikationspartner aktiviert")

        if _latest:
            _threshold_status = ""
            _age = _latest.age / 60 ## in minuten
            if threshold:
                _threshold_status = list(
                    map(lambda x: x[1], ## return only last
                        filter(lambda y: y[0] < _age, ## check threshold Texte
                            zip(threshold,(1,2)) ## warn 1 / crit 2
                        )
                    )
                )
            if not _threshold_status:
                if not _status:
                    _status = 0 ## ok
            else:
                _message = _("Snapshot ist zu alt")
                _status = _threshold_status[-1]
            if _latest != self.latest_snapshot:
                _message = _("Rollback zu altem Snapshot.'{0.snapshot}' nicht mehr vorhanden".format(self.latest_snapshot))
                _status = 2 ## crit

        if not self.checkzfs:
            _status = -1

        return {
            "source"        : source.dataset_name if source else "",
            "replica"       : self.dataset_name if source != self else "",
            "type"          : self.type,
            "autosnapshot"  : self.autosnapshot,
            "used"          : self.used,
            "available"     : self.available,
            "creation"      : (_latest.creation if _latest else 0) if source != self else self.creation,
            "count"         : len(self.snapshots.keys()),
            "snapshot"      : _latest.snapshot if _latest else "",
            "age"           : _latest.age if _latest else 0,
            "written"       : _latest.written if _latest else 0,
            "status"        : _status,
            "message"       : _message
        }

    def __repr__(self):
        return f"{self.dataset:25.25}{self.type}\n"

    def __str__(self):
        return f"{self.dataset:25.25}{self.type}  -snapshots: {self.lastsnapshot}\n"

class zfscheck(object):
    ZFSLIST_REGEX = re.compile("^(?P<dataset>.*?)(?:|@(?P<snapshot>.*?))\t(?P<type>\w*)\t(?P<creation>\d+)\t(?P<guid>\d+)\t(?P<used>\d+|-)\t(?P<available>\d+|-)\t(?P<written>\d+|-)\t(?P<autosnapshot>[-\w]+)\t(?P<checkzfs>[-\w]+)$",re.M)
    ZFS_DATASETS = {}
    ZFS_SNAPSHOTS = {}
    #VALIDCOLUMNS = ["source","replica","type","autosnap","snapshot","creation","guid","used","referenced","size","age","status","message"] ## valid columns
    VALIDCOLUMNS = zfs_dataset("","",0,0,0,"","","").get_info(None).keys() ## generate with dummy values
    DEFAULT_COLUMNS = ["status","source","replica","snapshot","age","count"] #,"message"] ## default columns
    DATEFORMAT = "%a %d.%b.%Y %H:%M"
    COLOR_CONSOLE = {
        0  : "\033[92m",  ## ok
        1  : "\033[93m",  ## warn
        2  : "\033[91m",  ## crit
        "reset" : "\033[0m"
    }
    COLUMN_NAMES = {  ## Namen frei editierbar
        "source"        : _("Quelle"),
        "snapshot"      : _("Snapshotname"),
        "creation"      : _("Erstellungszeit"),
        "type"          : _("Typ"),
        "age"           : _("Alter"),
        "count"         : _("Anzahl"),
        "used"          : _("genutzt"),
        "available"     : _("verfügbar"),
        "replica"       : _("Replikat"),
        "written"       : _("geschrieben"),
        "autosnapshot"  : _("Autosnapshot"),
        "message"       : _("Kommentar")
    }
    COLUMN_ALIGN = {
        "source"    : "<",
        "replica"   : "<",
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

    def __init__(self,remote,source,prefix='REPLICA',**kwargs):
        _start_time = time.time()
        self.remote = remote.split(",") if remote else [""] if source else []
        self.source = source.split(",") if source else [""]
        self.filter = None
        self.prefix = prefix.strip().replace(" ","_")
        self.rawdata = False
        self._overall_status = []
        self.sortreverse = False
        self._check_kwargs(kwargs)
        _data = self.get_data()
        if self.output == "text":
            print(self.table_output(_data))
        if self.output == "html":
            print( self.html_output(_data))
        if self.output == "mail":
            self.mail_output(_data)
        if self.output == "checkmk":
            print(self.checkmk_output(_data))
        if self.output == "json":
            print(self.json_output(_data))
        if self.output == "csv":
            print(self.csv_output(_data))

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

                if _v == ["*"]:
                    _default = self.VALIDCOLUMNS
                else:
                    for _column in _v:
                        if _column not in self.VALIDCOLUMNS:
                            raise Exception(_("ungültiger Spaltenname {0} ({1})").format(_v,",".join(self.VALIDCOLUMNS)))
                        _default.append(_column)
                _v = list(_default)

            if _k == "sort" and _v:
                ## sortierung desc wenn mit +
                if _v.startswith("+"):
                    self.sortreverse = True
                    _v = _v[1:]
                if _v not in self.VALIDCOLUMNS:
                    raise Exception("ungültiger Spaltenname: {0} ({1})".format(_v,",".join(self.VALIDCOLUMNS)))

            if _k == "threshold" and _v:
                _v = _v.split(",")
                ## todo tage etc
                _v = list(map(int,_v[:2])) ## convert zu int
                if len(_v) == 1:
                    _v = (float("inf"),_v[0])
                _v = sorted(_v)  ## kleinere Wert ist immer warn

            if _k == "filter" and _v:
                _v = re.compile(_v)

            if _k == "snapshotfilter" and _v:
                _v = re.compile(_v)

            setattr(self,_k,_v)

        ## funktionen zum anzeigen / muss hier da sonst kein self
        if not self.rawdata:
            self.COLUMN_MAPPER = {
                "creation"      : self.convert_ts_date,
                "age"           : self.seconds2timespan,
                "used"          : self.format_bytes,
                "available"     : self.format_bytes,
                "written"       : self.format_bytes,
                "autosnapshot"  : self.format_autosnapshot,
                "status"        : self.format_status
            }

    def get_data(self):
        #_data = self._call_proc()
        #self.get_local_snapshots(_data,remote=False)
        _remote_server = self.source + self.remote
        #pprint(_remote_server)
        for _remote in _remote_server:
            _remote = _remote.strip() if type(_remote) == str else None
            #print(f"S:{_remote}")
            _data = self._call_proc(_remote)
            for _entry in self._parse(_data):
                #if _entry.get("checkzfs") in ("0","false","ignore"):
                #    continue
                _dsname = "{0}#{dataset}".format(_remote,**_entry)
                if _entry.get("type") in ("volume","filesystem"):
                    if _remote in self.source:
                        self.ZFS_DATASETS[_dsname] = _dataset = zfs_dataset(**_entry,source=_remote)
                    #elif _remote in self.remote:
                    #    self.ZFS_DATASETS[_dsname] = _dataset = zfs_dataset(**_entry,remote=_remote)
                    else:
                        self.ZFS_DATASETS[_dsname] = _dataset = zfs_dataset(**_entry,remote=_remote)

                    continue
                _dataset = self.ZFS_DATASETS.get(_dsname)
                if not _dataset:
                    continue
                if self.snapshotfilter and not self.snapshotfilter.search(_entry.get("snapshot","")):
                    continue
                _snapshot = _dataset.add_snapshot(**_entry)
                _source_snapshot = self.ZFS_SNAPSHOTS.get(_snapshot.guid)
                if _source_snapshot:
                    _source_snapshot.add_replica(_snapshot)
                    continue

                if _remote in self.source: ## nur source snaps zur liste
                    self.ZFS_SNAPSHOTS[_snapshot.guid] = _snapshot

        _output = []
        for _dataset in self.ZFS_DATASETS.values():
            if self.filter and not self.filter.search(_dataset.dataset):
                continue
            if _dataset.remote in self.remote or _dataset.autosnapshot == 0:
                #pprint(_dataset)
                continue
            _dataset_info = _dataset.get_info(_dataset,threshold=self.threshold)
            self._overall_status.append(_dataset_info.get("status",-1))
            _output.append(_dataset_info)
            for _replica in _dataset.replica:
                _replica_info = _replica.get_info(_dataset,threshold=self.threshold)
                self._overall_status.append(_replica_info.get("status",-1))
                _output.append(_replica_info)

        return _output

    def _parse(self,data):
        for _match in self.ZFSLIST_REGEX.finditer(data):
            yield _match.groupdict()

    def _call_proc(self,remote=None):
        zfs_args = ["zfs", "list",
                "-t", "all",
                "-Hp",  ## script und numeric output
                "-o", "name,type,creation,guid,used,available,written,com.sun:auto-snapshot,tv.sysops:checkzfs",  ## attributes to show
                #"-r" ## recursive
        ]
        if remote:
            _privkeyoption = []
            if self.ssh_identity:
                _privkeyoption = ["-i",self.ssh_identity]
            _sshoptions = ["BatchMode=yes","PreferredAuthentications=publickey"]
            __sshoptions = []
            if self.ssh_extra_options:
                _sshoptions += self.ssh_extra_options.split(",")
            for _sshoption in _sshoptions:
                __sshoptions += ["-o", _sshoption]
            _parts = remote.split(":")
            _port = "22"  ## default port
            if len(_parts) > 1:
                remote = _parts[0]
                _port = _parts[1]
            zfs_args = ["ssh",
                remote, ## Hostname
                "-T",  ## dont allocate Terminal
                "-p" ,  _port
            ] + __sshoptions + _privkeyoption + zfs_args
        _proc = subprocess.Popen(zfs_args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False)
        _stdout, _stderr = _proc.communicate()
        if _proc.returncode > 0:
            if remote and _proc.returncode in (2,66,74,76): ## todo max try
                pass ## todo retry
                #time.sleep(30)
                #return self._call_proc(remote=remote)
            if remote and _proc.returncode in (2,65,66,67,69,70,72,73,74,76,78,79):
                ## todo set status ssh-error ....
                pass
            raise Exception(_stderr.decode(sys.stdout.encoding)) ## Raise Errorlevel with Error from proc
        return _stdout.decode(sys.stdout.encoding)

    def convert_ts_date(self,ts):
        return time.strftime(self.DATEFORMAT,time.localtime(ts))

    @staticmethod
    def format_status(val):
        return {-1:"ignored",0:"ok",1:"warn",2:"crit"}.get(val,"unknown")

    @staticmethod
    def format_autosnapshot(val):
        return {0:"deaktiviert",2:"aktiviert"}.get(val,"nicht konfiguriert")

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
            return ""
        _out = []
        for _item in self._datasort(data):
            #_out.append(" ".join([str(_item.get(_col,"")) for _col in ("source","replica","snapshot","creation","age")]))
            _status     = _item.get("status",3)
            _source     = _item.get("source","").replace(" ","_")
            _replica    = _item.get("replica","").strip()
            _creation   = _item.get("creation","0")
            _count      = _item.get("count","0")
            _age        = _item.get("age","0")
            _written    = _item.get("written","0")
            _available  = _item.get("available","0")
            _used       = _item.get("used","0")
            if _status == -1: # or _replica == "":
                continue
            if self.threshold:
                _warn = self.threshold[0] * 60
                _crit = self.threshold[1] * 60
                _threshold  = f"{_warn};{_crit}"
            else:
                _threshold  = ";"
            _msg        = _item.get("message","").strip()
            _msg = _msg if len(_msg) > 0 else "OK"
            _out.append(f"{_status} {self.prefix}:{_source} age={_age};{_threshold}|creation={_creation};;|file_size={_written};;|fs_used={_used};;|file_count={_count};; {_replica} - {_msg}")
        return "\n".join(_out)

    def table_output(self,data,color=True):
        if not data:
            return
        #print ("Max-Status: {0}".format(max(self._overall_status))) ## debug
        _header = data[0].keys() if not self.columns else self.columns
        _header_names = [self.COLUMN_NAMES.get(i,i) for i in _header]
        _converter = dict((i,self.COLUMN_MAPPER.get(i,(lambda x: str(x)))) for i in _header)
        _line_draw = (" | ","-+-","-")
        if color:
            _line_draw = (" ║ ","═╬═","═") ## mail quoted printable sonst base64 kein mailfilter
        _output_data = [_header_names]
        _line_status = []
        for _item in self._datasort(data):
            _line_status.append(_item.get("status"))
            _output_data.append([_converter.get(_col)(_item.get(_col,"")) for _col in _header])

        _maxwidth = [max(map(len,_col)) for _col in zip(*_output_data)] ## max column breite
        _format = _line_draw[0].join(["{{:{}{}}}".format(self.COLUMN_ALIGN.get(_h,">"),_w) for _h,_w in zip(_header,_maxwidth)])  ## format bilden mit min.max breite für gleiche spalten
        _line_print = False
        _out = []
        _status = -99 # max(self._overall_status) ## ??max status?? FIXME
        for _item in _output_data:
            if _line_print:
                _status = _line_status.pop(0)
            if color:
                _out.append(self.COLOR_CONSOLE.get(_status,"") + _format.format(*_item) + self.COLOR_CONSOLE.get("reset"))
            else:
                _out.append(_format.format(*_item))
            if not _line_print:
                _out.append(_line_draw[1].join(map(lambda x: x*_line_draw[2],_maxwidth))) ## trennlinie
                _line_print = True
        return "\n".join(_out)

    def html_output(self,data):
        if not data:
            return ""
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
        _hostname = socket.getfqdn()
        _users = open("/etc/pve/user.cfg","rt").read()
        _email = "root@{0}".format(_hostname)
        _emailmatch = re.search("^user:root@pam:.*?:(?P<mail>[\w.]+@[\w.]+):.*?$",_users,re.M)
        if _emailmatch:
            _email = _emailmatch.group(1)
            #raise Exception("No PVE User Email found")
        _msg = EmailMessage()
        _msg.set_content(self.table_output(data,color=False))
        #_msg.add_alternative(self.html_output(data),subtype="html") ## FIXME
        #_attach = MIMEApplication(self.csv_output(data),Name="zfs-check_{0}.csv".format(_hostname))
        #_attach["Content-Disposition"] = "attachement; filename=zfs-check_{0}.csv".format(_hostname)
        #_msg.attach(_attach)
        _msg["From"] = "ZFS-Checkscript {0} <root@{0}".format(_hostname)
        _msg["To"] = _email
        _msg["Date"] = formatdate(localtime=True)
        _msg["x-checkzfs-status"] = str(max(self._overall_status))
        _msg["Subject"] = "ZFS-Check {0}".format(_hostname.split(".")[0])
        _stderr, _stdout = (subprocess.PIPE,subprocess.PIPE)
        subprocess.run(["/usr/sbin/sendmail","-t","-oi"], input=_msg.as_bytes() ,stderr=_stderr,stdout=_stdout)

    def csv_output(self,data,separator=";"):
        if not data:
            return ""
        _header = data[0].keys() ## alles
        _header_names = [self.COLUMN_NAMES.get(i,i) for i in _header]
        _converter = dict((i,self.COLUMN_MAPPER.get(i,(lambda x: str(x)))) for i in _header)
        _output = [separator.join(_header_names)]
        for _item in self._datasort(data):
            _output.append(separator.join([_converter.get(_col)(_item.get(_col,"")) for _col in _header]))

        return "\n".join(_output)

    def json_output(self,data):
        return json.dumps(data)

if __name__ == "__main__":
    import argparse
    _parser = argparse.ArgumentParser("Tool to check ZFS Replication age\n##########################################\n")
    _parser.add_argument('--remote',type=str,
                help=_("SSH Connection Data user@host"))
    _parser.add_argument('--source',type=str,
                help=_("SSH Connection Data user@host for source"))
    _parser.add_argument("--filter",type=str,
                help=_("Regex Filter Datasets"))
    _parser.add_argument("--snapshotfilter",type=str,
                help=_("Regex Filter Snapshot"))
    _parser.add_argument("--output",type=str,default="text",choices=["html","text","mail","checkmk","json","csv"],
                help=_("Ausgabeformat"))
    _parser.add_argument("--columns",type=str,
                help=_("Zeige nur folgende Spalten ({0})".format(",".join(zfscheck.VALIDCOLUMNS))))
    _parser.add_argument("--sort",type=str,choices=zfscheck.VALIDCOLUMNS,
                help=_("Sortiere nach Spalte"))
    _parser.add_argument("--threshold",type=str,
                help=_("Grenzwerte für Alter von Snapshots warn,crit"))
    _parser.add_argument("--rawdata",action="store_true",
               help=_("zeigt Daten als Zahlen"))
    _parser.add_argument("--prefix",type=str,default='REPLICA',
               help=_("Prefix für check_mk Service (keine Leerzeichen)"))
    _parser.add_argument("--ssh-identity",type=str,
                help=_("Pfad zum ssh private key"))
    _parser.add_argument("--ssh-extra-options",type=str,
                help=_("zusätzliche SSH Optionen mit Komma getrennt (HostKeyAlgorithms=ssh-rsa)"))
    args = _parser.parse_args()
    _is_checkmk_plugin = os.path.dirname(os.path.abspath(__file__)).endswith("check_mk_agent/local")
    if _is_checkmk_plugin:
        try:
            _config_regex = re.compile("^(disabled|source|remote|prefix|filter|threshold|snapshotfilter|ssh-identity|ssh-extra-options):\s*(.*?)(?:\s+#|$)",re.M)
            _basename = os.path.basename(__file__).split(".")[0]
            _rawconfig = open(f"/etc/check_mk/{_basename}","rt").read()
            for _k,_v in _config_regex.findall(_rawconfig):
                if _k == "disabled" and _v.lower().strip() in ( "1","yes","true"):
                    os._exit(0)
                args.__dict__[_k.replace("-","_")] = _v.strip()
        except:
            pass
        args.output = "checkmk"
        #sys.stderr.write(repr(args.__dict__))
    try:
        zfscheck(**args.__dict__)
    except KeyboardInterrupt:
        print("")
        sys.exit(0)
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

