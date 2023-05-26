#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8:noet
##  Copyright 2023 sysops.tv ;-)
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

VERSION = 4.10

### for check_mk usage link or copy binary to check_mk_agent/local/checkzfs
### create /etc/check_mk/checkzfs ## the config file name matches the filename in check_mk_agent/local/
###         to create a diffent set, link script to check_mk_agent/local/checkzfs2 and create /etc/check_mk/checkzfs2
###
### source: host1                   # [optional] comma seperated hostnames to check for source
### remote: host1                   # [optional]     "              "           "       remote
### prefix: host1                   # [optional] Prefix for check_mk Servicename - default REPLICA
### filter: rpool/data|replica      # [optional] regex filter to match source
### replicafilter: remote           # [optional] regex filter to match for replica snapshots
### snapshotfilter:                 # [optional] regex filter to match snapshot name
### threshold: 20,40                # [optional] threshold warn,crit in minutes
### maxsnapshots: 60,80             # [optional] threshold maximum of snapshots warn,crit
### ssh-extra-options:              # [optional] comma seperated ssh options like added with -o 
### ssh-identity: /path/to/priv.key # [optional] path to ssh private key
### disabled: 1                     # [optional] disable the script with this config 
### legacyhosts: host1              # [optional] use an external script zfs_legacy_list to get snapshots with guid and creation at lease

## Regex Tips: 
## 'Raid5[ab]\/(?!Rep_|Swap-)\w+' everything from Raid5a or Raid5b not start with Rep_ or Swap-


##
##!/bin/bash
## legacy script example to put in path as zfs_legacy_list to for host with missing written attribute and list option -p
# for snapshot in $(zfs list -H -t all -o name);
# do
#     echo -ne "$snapshot"
#     zfs get -H -p type,creation,guid,used,available,userrefs,com.sun:auto-snapshot,tv.sysops:checkzfs $snapshot |  awk '{print $3}'|
#         while IFS= read -r line; do
#             echo -ne "\t${line}"
#         done
#     echo  ""
# done

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
    def __init__(self,dataset_obj,snapshot,creation,guid,written,origin,**kwargs):
        self.replica = []
        self.dataset_obj = dataset_obj
        self.snapshot = snapshot
        self.creation = int(creation)
        self.age = int(time.time() - self.creation)
        self.written = int(written)
        self.origin = origin
        self.guid = guid

    def add_replica(self,snapshot):
            self.replica.append(snapshot) ## den snapshot als replica hinzu
            self.dataset_obj.add_replica(snapshot.dataset_obj) ## als auch dem dataset

    def __repr__(self):
        return f"{self.guid} {self.dataset_obj.dataset_name} {self.snapshot}\n"

    def __str__(self):
        return f"{self.guid} {self.snapshot}\n"



class zfs_dataset(object):
    def __init__(self,dataset,guid,used,available,creation,type,autosnapshot,checkzfs,remote=None,source=None,**kwargs):
        self.checkzfs = checkzfs not in ("false","ignore")  ## ignore wenn tv.sysops:checkzfs entweder false oder ignore (ignore macht es überischtlicher)
        self.snapshots = {}
        self.remote = remote
        self.is_source = source
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
        _obj = zfs_snapshot(self,**kwargs) ## neuen snapshot mit parametern erstellen
        self.snapshots[_obj.guid] = _obj ## zu lokalen snapshots diesem DS hinzu
        return _obj ## snapshot objeckt zurück

    def add_replica(self,ds_object,**kwargs):
        self.replica.add(ds_object) 

    def _get_latest_snapshot(self,source=None):
        _snapshots = self.sorted_snapshots()
        if source: ## wenn anderes dataset übergeben dann nur snapshots zurück die auch auf der anderen seite (mit gleicher guid) vorhanden sind
            _snapshots = list(filter(lambda x: x.guid in source.snapshots.keys(),_snapshots))
        return _snapshots[0] if _snapshots else None ## letzten gemeinsamen snapshot zurück 

    def sorted_snapshots(self):
        return sorted(self.snapshots.values(), key=lambda x: x.age) ## snapshots nach alter sortiert

    @property
    def dataset_name(self): ## namen mit host prefixen
        if self.remote:
            return f"{self.remote}#{self.dataset}"
        return self.dataset

    @property
    def latest_snapshot(self): ## letzten snapshot
        if self.snapshots:
            return self.sorted_snapshots()[0]


    def get_info(self,source,threshold=None,maxsnapshots=None,ignore_replica=False):
        _latest = self._get_latest_snapshot(source if source != self else None) ## wenn das source dataset nicht man selber ist
        _status = -1
        _has_zfs_autosnapshot = any(map(lambda x: str(x.snapshot).startswith("zfs-auto-snap_"),self.snapshots.values()))
        _message = ""
        if source == self:
            if not self.replica and ignore_replica == False:
                _status = 1 ## warn
                _message = _("kein Replikat gefunden")
            if self.autosnapshot == 2 and _has_zfs_autosnapshot:
                _status = 1 ## warn
                _message = _("com.sun:auto-snapshot ist auf der Quelle auf true und wird evtl. mit repliziert")
        else:
            if _has_zfs_autosnapshot:  ## nur auf systemen mit zfs-aut-snapshot
                if self.autosnapshot == 1:
                    _status = 1 ## warn
                    _message = _("com.sun:auto-snapshot ist nicht false")
                elif self.autosnapshot == 2:
                    _status = 2 ## crit
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
                if _status == -1:
                    _status = 0 ## ok
            else:
                _message = _("Snapshot ist zu alt")
                _status = _threshold_status[-1]
            if _latest != self.latest_snapshot:
                _message = _("Rollback zu altem Snapshot. - '{0.snapshot}' nicht mehr vorhanden".format(self.latest_snapshot))
                _status = 2 ## crit

        if maxsnapshots:
            _maxsnapshot_status = list(
                map(lambda x: x[1],
                    filter(lambda y: y[0] < len(self.snapshots.keys()),
                        zip(maxsnapshots,(1,2))
                    )
                )
            )
            if _maxsnapshot_status:
                if _maxsnapshot_status[-1] > _status:
                    _message = _("zu viele Snapshots")
                    _status = _maxsnapshot_status[-1]
        if not self.checkzfs:
            _status = -1

        return {  ## neues object zurück was die attribute enthält die wir über columns ausgeben
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
            "origin"        : _latest.origin if _latest else "",
            "guid"          : _latest.guid if _latest else "",
            "status"        : _status,
            "message"       : _message
        }
        
    def __repr__(self):
        return f"{self.is_source}-{self.dataset_name:25.25}{self.type}\n"

    def __str__(self):
        return f"{self.dataset:25.25}{self.type}  -snapshots: {self.lastsnapshot}\n"

class no_regex_class(object):
    def search(*args):
        return True

class negative_regex_class(object):
    def __init__(self,compiled_regex):
        self.regex = compiled_regex
    def search(self,text):
        return not self.regex.search(text)

class zfscheck(object):
    ZFSLIST_REGEX = re.compile("^(?P<dataset>.*?)(?:|@(?P<snapshot>.*?))\t(?P<type>\w*)\t(?P<creation>\d+)\t(?P<guid>\d+)\t(?P<used>\d+|-)\t(?P<available>\d+|-)\t(?P<written>\d+|-)\t(?P<origin>.*?)\t(?P<autosnapshot>[-\w]+)\t(?P<checkzfs>[-\w]+)$",re.M)
    ZFS_DATASETS = {}
    ZFS_SNAPSHOTS = {}
    #VALIDCOLUMNS = ["source","replica","type","autosnap","snapshot","creation","guid","used","referenced","size","age","status","message"] ## valid columns
    VALIDCOLUMNS = zfs_dataset("","",0,0,0,"","","").get_info(None).keys() ## generate with dummy values
    DEFAULT_COLUMNS = ["status","source","replica","snapshot","age","count"] #,"message"] ## default columns
    DATEFORMAT = "%a %d.%b.%Y %H:%M"
    COLOR_CONSOLE = {
        0  : "\033[92m",  ## ok
        1  : "\033[93m",  ## warn  ## hier ist das hässliche gelb auf der weißen console .... GOFOR themes!!!1111
        2  : "\033[91m",  ## crit
        "reset" : "\033[0m"
    }
    COLUMN_NAMES = {  ## Namen frei editierbar
        "source"        : _("Quelle"),
        "snapshot"      : _("Snapshotname"),
        "creation"      : _("Erstellungszeit"),
        "type"          : _("Typ"),
        "age"           : _("Alter"),
        "guid"          : _("GUID"),
        "count"         : _("Anzahl"),
        "used"          : _("genutzt"),
        "available"     : _("verfügbar"),
        "replica"       : _("Replikat"),
        "written"       : _("geschrieben"),
        "origin"        : _("Ursprung"),
        "autosnapshot"  : _("Autosnapshot"),
        "message"       : _("Kommentar")
    }
    COLUMN_ALIGN = {  ## formatierung align - python string format
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

    def __init__(self,remote,source,sourceonly,legacyhosts,output,mail=None,prefix='REPLICA',debug=False,**kwargs):
        _start_time = time.time()
        self.remote_hosts = remote.split(",") if remote else [""] if source and not sourceonly else [] ## wenn nicht und source woanders ... "" (also lokal) als remote
        self.source_hosts = source.split(",") if source else [""] ## wenn nix dann "" als local
        self.legacy_hosts = legacyhosts.split(",") if legacyhosts else []
        self.sourceonly = sourceonly
        self.filter = None
        self.debug = debug
        self.print_debug(f"Version: {VERSION}")
        self.prefix = prefix.strip().replace(" ","_") ## service name bei checkmk leerzeichen durch _ ersetzen
        self.rawdata = False
        self.mail_address = mail
        self._overall_status = []
        self.sortreverse = False
        self.output = output if mail == None else "mail"
        self.print_debug(f"set attribute: remote -> {self.remote_hosts!r}")
        self.print_debug(f"set attribute: source -> {self.source_hosts!r}")
        self.print_debug(f"set attribute: sourceonly -> {sourceonly!r}")
        self.print_debug(f"set attribute: prefix -> {prefix!r}")
        if legacyhosts:
            self.print_debug(f"set attribute: legacyhosts -> {self.legacy_hosts}")
        self._check_kwargs(kwargs)
        self.print_debug(f"set attribute: output -> {self.output!r}")
        self.get_data()
        if self.output != "snaplist":
            _data = self.get_output()
        else:
            print(self.get_snaplist())
        if self.output == "text" or self.output == "":
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

    def _check_kwargs(self,kwargs): ## alle argumente prüfen und als attribute zuordnen
        ## argumente überprüfen

        for _k,_v in kwargs.items():
            self.print_debug(f"set attribute: {_k} -> {_v!r}")

            if _k == "columns":
                if self.output == "snaplist":
                    _default = ["status","source","snapshot","replica","guid","age"]
                else:
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

            if _k == "maxsnapshots" and _v:
                _v = _v.split(",")
                ## todo tage etc
                _v = list(map(int,_v[:2])) ## convert zu int
                if len(_v) == 1:
                    _v = (float("inf"),_v[0])
                _v = sorted(_v)  ## kleinere Wert ist immer warn

            if _k in ("filter","snapshotfilter","replicafilter"):
                if _v:
                    if _v.startswith("!"):
                        _v = negative_regex_class(re.compile(_v[1:]))
                    else:
                        _v = re.compile(_v)
                else:
                    _v = no_regex_class() ### dummy klasse .search immer True - spart abfrage ob filter vorhanden

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
        _hosts_checked = []
        _remote_servers = set(self.source_hosts + self.remote_hosts) ### no duplicate connection
        _remote_data = {}
        _start_time = time.time()
        _iteration = 0
        for _remote in _remote_servers: ## erstmal rohdaten holen
            _remote = _remote.strip() if type(_remote) == str else None ## keine leerzeichen, werden von ghbn mit aufgelöst 
            _remote_data[_remote] = self._call_proc(_remote)
            _iteration+=1

        _matched_snapshots = 0
        _filtered_snapshots = 0
        for _remote,_rawdata in _remote_data.items(): ## allen source datasets erstmal snapshots hinzu und bei den anderen dataset anlegen
            for _entry in self._parse(_rawdata):
                _iteration+=1
                _dsname = "{0}#{dataset}".format(_remote,**_entry)  ## name bilden
                _is_source = bool(_remote in self.source_hosts and self.filter.search(_dsname))
                if _entry.get("type") in ("volume","filesystem"): ## erstmal keine snapshots
                    self.ZFS_DATASETS[_dsname] = zfs_dataset(**_entry,remote=_remote,source=_is_source)
                    continue ## nix mehr zu tun ohne snapshot
                if not _is_source:
                    continue
                ## snapshots
                if not self.snapshotfilter.search(_entry.get("snapshot","")): ## wenn --snapshotfilter gesetzt und kein match
                    _filtered_snapshots+=1
                    continue 
                _matched_snapshots+=1
                _dataset = self.ZFS_DATASETS.get("{0}#{dataset}".format(_remote,**_entry))
                try:
                    _snapshot = _dataset.add_snapshot(**_entry)
                except:
                    pass
                    raise
                self.ZFS_SNAPSHOTS[_snapshot.guid] = _snapshot
        _execution_time = time.time() - _start_time

        if self.sourceonly == True:
            return

        for _remote,_rawdata in _remote_data.items(): ## jetzt nach replica suchen
            for _entry in self._parse(_rawdata):  ## regex geparste ausgabe von zfs list 
                _iteration+=1
                if _entry.get("type") != "snapshot": ## jetzt nur die snapshots
                    continue
                _dataset = self.ZFS_DATASETS.get("{0}#{dataset}".format(_remote,**_entry))
                if _dataset.is_source:
                    continue ## ist schon source
                _snapshot = _dataset.add_snapshot(**_entry) ## snapshot dem dataset hinzufügen .. eigentliche verarbeitung Klasse oben snapshot object wird zurück gegeben
                _source_snapshot = self.ZFS_SNAPSHOTS.get(_snapshot.guid) ## suchen ob es einen source gibt
                if _source_snapshot: ## wenn es schon eine gleiche guid gibt
                    if self.replicafilter.search(_dataset.dataset_name):
                        _source_snapshot.add_replica(_snapshot) ## replica hinzu

        self.print_debug(f"computation time: {_execution_time:0.2f} sec /  iterations: {_iteration} / matched snapshots: {_matched_snapshots} / filtered snaphots: {_filtered_snapshots}")


    def get_snaplist(self):
        _output = []
        for _dataset in self.ZFS_DATASETS.values():
            if not _dataset.is_source: ## nur source im filter
                continue
            for _snapshot in _dataset.snapshots.values():
                _replicas = list(map(lambda x: x.dataset_obj.dataset_name,_snapshot.replica))
                _output.append({
                    "status"        : 1 if len(_replicas) == 0 else 0,
                    "source"        : _dataset.dataset_name,
                    "snapshot"      : _snapshot.snapshot,
                    "replica"       : ",".join(_replicas),
                    "guid"          : _snapshot.guid,
                    "age"           : _snapshot.age,
                    "written"       : _snapshot.written,
                })
                
                #print(f"{_snapshot.snapshot}{_snapshot.guid}{_snapshot.replica}")
        return self.table_output(_output)

    def get_output(self):
        _output = []
        for _dataset in self.ZFS_DATASETS.values(): ## alle Datasets durchgehen die als source gelistet werden sollen
            if not _dataset.is_source:  ## wenn --filter gesetzt
                continue
            #if _dataset.remote in self.remote_hosts:## or _dataset.autosnapshot == 0:  ## wenn das dataset von der remote seite ist ... dann weiter oder wenn autosnasphot explizit aus ist ... dann nicht als source hinzufügen
            #    continue
            _dataset_info = _dataset.get_info(_dataset,threshold=self.threshold,maxsnapshots=self.maxsnapshots,ignore_replica=self.sourceonly)
            self._overall_status.append(_dataset_info.get("status",-1))  ## alle stati für email overall status
            _output.append(_dataset_info)
            if self.sourceonly == True:
                continue
            for _replica in _dataset.replica: ## jetzt das dataset welches als source angezeigt wird (alle filter etc entsprochen nach replika durchsuchen
                #if not self.replicafilter.search(_replica.dataset_name):
                #    continue
                _replica_info = _replica.get_info(_dataset,threshold=self.threshold,maxsnapshots=self.maxsnapshots)  ## verarbeitung ausgabe aus klasse 
                self._overall_status.append(_replica_info.get("status",-1)) ## fehler aus replica zu overall status für mail adden
                _output.append(_replica_info)

        return _output

    def _parse(self,data):
        for _match in self.ZFSLIST_REGEX.finditer(data):
            yield _match.groupdict()

    def _call_proc(self,remote=None):
        ZFS_ATTRIBUTES = "name,type,creation,guid,used,available,written,origin,com.sun:auto-snapshot,tv.sysops:checkzfs" ## wenn ändern dann auch regex oben anpassen
        ### eigentlicher zfs aufruf, sowohl local als auch remote
        zfs_args = ["zfs", "list", 
                "-t", "all",
                "-Hp",  ## script und numeric output
                "-o", ZFS_ATTRIBUTES,  ## attributes to show
                #"-r" ## recursive
        ]
        if remote: ##wenn remote ssh adden
            if remote in self.legacy_hosts:
                zfs_args = ["zfs_legacy_list"]
            _privkeyoption = []
            if self.ssh_identity:
                _privkeyoption = ["-i",self.ssh_identity]
            _sshoptions = ["BatchMode=yes","PreferredAuthentications=publickey"]
            __sshoptions = []
            if self.ssh_extra_options:
                _sshoptions += self.ssh_extra_options.split(",")
            for _sshoption in _sshoptions:
                __sshoptions += ["-o", _sshoption] ## alle ssh optionen brauchen -o einzeln 
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
        self.print_debug("call proc: '{0}'".format(" ".join(zfs_args)))
        _start_time = time.time()
        _proc = subprocess.Popen(zfs_args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False) #aufruf prog entweder lokal oder mit ssh
        _stdout, _stderr = _proc.communicate()
        _execution_time = time.time() - _start_time
        _lines_returned = len(_stdout.split())
        self.print_debug(f"returncode: {_proc.returncode} / Executiontime: {_execution_time:0.2f} sec / Lines: {_lines_returned}")
        if _proc.returncode > 0: ## wenn fehler
            if remote and _proc.returncode in (2,66,74,76): ## todo max try
                pass ## todo retry ## hier könnte man es mehrfach versuchen wenn host nicht erreichbar aber macht bei check_mk keinen sinn
                #time.sleep(30)
                #return self._call_proc(remote=remote)
            if remote and _proc.returncode in (2,65,66,67,69,70,72,73,74,76,78,79): ## manche error ignorieren hauptsächlich ssh
                ## todo set status ssh-error ....
                pass ## fixme ... hostkeychange evtl fehler raisen o.ä damit check_mk das mitbekommt
            raise Exception(_stderr.decode(sys.stdout.encoding)) ## Raise Errorlevel with Error from proc -- kann check_mk stderr lesen? sollte das nach stdout?
        return _stdout.decode(sys.stdout.encoding) ## ausgabe kommt als byte wir wollen str

    def convert_ts_date(self,ts,dateformat=None):
        if dateformat:
            return time.strftime(dateformat,time.localtime(ts))
        else:
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
            _status     = _item.get("status",3)
            _source     = _item.get("source","").replace(" ","_")
            _replica    = _item.get("replica","").strip()
            _creation   = _item.get("creation","0")
            _count      = _item.get("count","0")
            _age        = _item.get("age","0")
            _written    = _item.get("written","0")
            _available  = _item.get("available","0")
            _used       = _item.get("used","0")
            if _status == -1: ## tv.sysops:checkzfs=ignore wollen wir nicht
                continue
            if self.maxsnapshots:
                _warn = self.maxsnapshots[0]
                _crit = self.maxsnapshots[1]
                _maxsnapshots = f"{_warn};{_crit}".replace("inf","")
                #if _status == 0:
                #    _status = "P"
            else:
                _maxsnapshots = ";"
            if self.threshold:
                _warn = self.threshold[0] * 60
                _crit = self.threshold[1] * 60
                _threshold  = f"{_warn};{_crit}".replace("inf","")
            else:
                _threshold  = ";"
            _msg        = _item.get("message","").strip()
            _msg = _msg if len(_msg) > 0 else "OK" ## wenn keine message ... dann OK
            _out.append(f"{_status} {self.prefix}:{_source} age={_age};{_threshold}|creation={_creation};;|file_size={_written};;|fs_used={_used};;|file_count={_count};{_maxsnapshots} {_replica} - {_msg}")
        
        if self.piggyback != "":
            _out.insert(0,f"<<<<{self.piggyback}>>>>\n<<<local:sep(0)>>>")
            _out.append("<<<<>>>>")
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

    def html_output(self,data,columns=None):
        if not data:
            return ""
        _header = data[0].keys() if not self.columns else self.columns
        _header_names = [self.COLUMN_NAMES.get(i,i) for i in _header]
        _converter = dict((i,self.COLUMN_MAPPER.get(i,(lambda x: str(x)))) for i in _header)
        _hostname = socket.getfqdn()
        _now = self.convert_ts_date(time.time(),'%Y-%m-%d %H:%M:%S')
        _out = []
        _out.append("<html>")
        _out.append("<head>")
        _out.append("<meta name='color-scheme' content='only'>")
        _out.append("<style type='text/css'>")
        _out.append("html{height:100%%;width:100%%;}")
        _out.append("body{color:black;width:auto;padding-top:2rem;}")
        _out.append("h1,h2{text-align:center;}")
        _out.append("table{margin: 2rem auto;}")
        _out.append("table,th,td {border:1px solid black;border-spacing:0;border-collapse:collapse;padding:.2rem;}")
        _out.append("th{text-transform:capitalize}")
        _out.append("td:first-child{text-align:center;font-weight:bold;text-transform:uppercase;}")
        _out.append("td:last-child{text-align:right;}")
        _out.append(".warn{background-color:yellow;}")
        _out.append(".crit{background-color:red;color:black;}")
        _out.append("</style>")
        _out.append("<title>Check ZFS</title></head><body>")
        _out.append(f"<h1>{_hostname}</h1><h2>{_now}</h2>")
        _out.append("<table>")
        _out.append("<tr><th>{0}</th></tr>".format("</th><th>".join(_header_names)))
        for _item in self._datasort(data):
            _out.append("<tr class='{1}'><td>{0}</td></tr>".format("</td><td>".join([_converter.get(_col)(_item.get(_col,"")) for _col in _header]),_converter["status"](_item.get("status","0"))))
        _out.append("</table></body></html>")
        return "".join(_out)

    def mail_output(self,data):
        _hostname = socket.getfqdn()
        _email = self.mail_address
        if not _email:
            _users = open("/etc/pve/user.cfg","rt").read()
            _email = "root@{0}".format(_hostname)
            _emailmatch = re.search("^user:root@pam:.*?:(?P<mail>[\w.]+@[\w.]+):.*?$",_users,re.M)
            if _emailmatch:
                _email = _emailmatch.group(1)
            #raise Exception("No PVE User Email found")
        _msg = EmailMessage()
        _msg.set_content(self.table_output(data,color=False))
        _msg.add_alternative(self.html_output(data),subtype="html") ## FIXME wollte  irgendwie nicht als multipart ..
        #_attach = MIMEApplication(self.csv_output(data),Name="zfs-check_{0}.csv".format(_hostname))
        #_attach["Content-Disposition"] = "attachement; filename=zfs-check_{0}.csv".format(_hostname)
        #_msg.attach(_attach)
        _msg["From"] = "ZFS-Checkscript {0} <root@{0}".format(_hostname)
        _msg["To"] = _email
        _msg["Date"] = formatdate(localtime=True)
        _msg["x-checkzfs-status"] = str(max(self._overall_status))
        _msg["Subject"] = "ZFS-Check -{0}- {1}".format(self.format_status(max(self._overall_status)).upper(),_hostname.split(".")[0])
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

    def print_debug(self,msg,*args,**kwargs):
        if self.debug:
            sys.stderr.write(f"DEBUG: {msg}\n")
            sys.stderr.flush()

if __name__ == "__main__":
    import argparse
    _parser = argparse.ArgumentParser(f"Tool to check ZFS Replication age\nVersion: {VERSION}\n##########################################\n")
    _parser.add_argument('--remote',type=str,
                help=_("SSH Connection Data user@host"))
    _parser.add_argument('--source',type=str,
                help=_("SSH Connection Data user@host for source"))
    _parser.add_argument("--filter",type=str,
                help=_("Regex Filter Datasets die als Source aufgenommen werden sollen (z.B: rpool/prod)"))
    _parser.add_argument("--snapshotfilter",type=str,
                help=_("Regex Filter Snapshot snapshots die überhaupt benutzt werden (z.B. daily)"))
    _parser.add_argument("--replicafilter",type=str,
                help=_("Regex Filter wo nach replikaten gesucht werden soll (z.B. remote)"))
    _parser.add_argument("--output",type=str,default="",choices=["html","text","mail","checkmk","json","csv","snaplist"],
                help=_("Ausgabeformat"))
    _parser.add_argument("--columns",type=str,
                help=_("Zeige nur folgende Spalten ({0})".format(",".join(zfscheck.VALIDCOLUMNS))))
    _parser.add_argument("--sort",type=str,choices=zfscheck.VALIDCOLUMNS,
                help=_("Sortiere nach Spalte"))
    _parser.add_argument("--sourceonly",default=False,action="store_true",
                help=_("Nur Snapshot-Alter prüfen"))
    _parser.add_argument("--mail",type=str,
                help=_("Email für den Versand"))
    _parser.add_argument("--config",dest="config_file",type=str,default="",
                help=_("Config File"))
    _parser.add_argument("--threshold",type=str,
                help=_("Grenzwerte für Alter von Snapshots warn,crit"))
    _parser.add_argument("--maxsnapshots",type=str,
                help=_("Grenzwerte für maximale Snapshots warn,crit"))
    _parser.add_argument("--rawdata",action="store_true",
               help=_("zeigt Daten als Zahlen"))
    _parser.add_argument("--snaplist","-s",action="store_const",dest="output",const="snaplist",
                help=_("kurz für --output snaplist"))
    _parser.add_argument("--legacyhosts",type=str,
                help=_("Hosts der Source und Remote die kein zfs list mit allen Parametern können rufen zfs_legacy_list auf"))
    _parser.add_argument("--prefix",type=str,default='REPLICA',
               help=_("Prefix für check_mk Service (keine Leerzeichen)"))
    _parser.add_argument("--ssh-identity",type=str,
                help=_("Pfad zum ssh private key"))
    _parser.add_argument("--piggyback",type=str,default="",
                help=_("Zuordnung zu anderem Host bei checkmk"))
    _parser.add_argument("--ssh-extra-options",type=str,
                help=_("zusätzliche SSH Optionen mit Komma getrennt (HostKeyAlgorithms=ssh-rsa)"))
    _parser.add_argument("--update",nargs="?",const="main",type=str,choices=["main","testing"],
        help=_("check for update"))
    _parser.add_argument("--debug",action="store_true",
                help=_("debug Ausgabe"))
    args = _parser.parse_args()

    CONFIG_KEYS="disabled|source|sourceonly|piggyback|remote|legacyhosts|prefix|filter|replicafilter|threshold|maxsnapshots|snapshotfilter|ssh-identity|ssh-extra-options"
    _config_regex = re.compile(f"^({CONFIG_KEYS}):\s*(.*?)(?:\s+#|$)",re.M)
    _basename = os.path.basename(__file__).split(".")[0]  ## name für config ermitteln aufgrund des script namens
    _is_checkmk_plugin = os.path.dirname(os.path.abspath(__file__)).find("/check_mk_agent/local") > -1 ## wenn im check_mk ordner
    if _is_checkmk_plugin:
        try: ## parse check_mk options
            _check_mk_configdir = "/etc/check_mk"
            if not os.path.isdir(_check_mk_configdir):
                _check_mk_configdir = os.environ["MK_CONFDIR"]
            args.config_file = f"{_check_mk_configdir}/{_basename}"
            if not os.path.exists(args.config_file):  ### wenn checkmk aufruf und noch keine config ... default erstellen
                if not os.path.isdir(_check_mk_configdir):
                    os.mkdir(_check_mk_configdir)
                with open(args.config_file,"wt") as _f: ## default config erstellen
                    _f.write("## config for checkzfs check_mk")
                    _f.write("\n".join([f"# {_k}:" for _k in CONFIG_KEYS.split("|")]))
                    _f.write("\n")
                print(f"please edit config {args.config_file}")
                os._exit(0)
        except:
            pass
        args.output = "checkmk" if not args.output else args.output
    _is_zabbix_plugin = os.path.dirname(os.path.abspath(__file__)).find("/zabbix/scripts") > -1 ## wenn im check_mk ordner
    if _is_zabbix_plugin:
        try: ## parse check_mk options
            args.config_file = f"/etc/zabbix/checkzfs-{_basename}"
            if not os.path.exists(args.config_file):  ### wenn checkmk aufruf und noch keine config ... default erstellen
                if not os.path.isdir("/etc/zabbix"):
                    os.mkdir("/etc/zabbix")
                with open(args.config_file,"wt") as _f: ## default config erstellen
                    _f.write("## config for checkzfs zabbix")
                    _f.write("\n".join([f"# {_k}:" for _k in CONFIG_KEYS.split("|")]))
                    _f.write("\n")
                print(f"please edit config {args.config_file}")
                os._exit(0)
        except:
            pass
        args.output = "json" if not args.output else args.output

    if args.config_file:
        _rawconfig = open(args.config_file,"rt").read()
        for _k,_v in _config_regex.findall(_rawconfig):
            if _k == "disabled" and _v.lower().strip() in ( "1","yes","true"): ## wenn disabled dann ignorieren check wird nicht durchgeführt
                os._exit(0)
            if _k == "sourceonly":
                args.sourceonly = bool(_v.lower().strip() in ( "1","yes","true"))
            elif _k == "prefix":
                args.__dict__["prefix"] = _v.strip()
            elif not args.__dict__.get(_k.replace("-","_"),None):
                args.__dict__[_k.replace("-","_")] = _v.strip()

    try:
        if args.update:
            import requests
            import hashlib
            import base64
            from datetime import datetime
            import difflib
            _github_req = requests.get(f"https://api.github.com/repos/bashclub/check-zfs-replication/contents/checkzfs.py?ref={args.update}")
            if _github_req.status_code != 200:
                raise Exception("Github Error")
            _github_version = _github_req.json()
            _github_last_modified = datetime.strptime(_github_req.headers.get("last-modified"),"%a, %d %b %Y %X %Z")
            _new_script = base64.b64decode(_github_version.get("content")).decode("utf-8")
            _new_version = re.findall("^VERSION\s*=\s*([0-9.]*)",_new_script,re.M)
            _new_version = float(_new_version[0]) if _new_version else 0.0
            _script_location = os.path.realpath(__file__)
            _current_last_modified = datetime.fromtimestamp(int(os.path.getmtime(_script_location)))
            with (open(_script_location,"rb")) as _f:
                _content = _f.read()
            _current_sha = hashlib.sha1(f"blob {len(_content)}\0".encode("utf-8") + _content).hexdigest()
            _content = _content.decode("utf-8")
            if _current_sha == _github_version.get("sha"):
                print(f"allready up to date {_current_sha}")
                sys.exit(0)
            else:
                if VERSION == _new_version:
                    print("same Version but checksums mismatch")
                elif VERSION > _new_version:
                    print(f"ATTENTION: Downgrade from {VERSION} to {_new_version}")
            while True:
                try:
                    _answer = input(f"Update {_script_location} to {_new_version} (y/n) or show difference (d)? ")
                except KeyboardInterrupt:
                    print("")
                    sys.exit(0)
                if _answer in ("Y","y","yes","j","J"):
                    with open(_script_location,"wb") as _f:
                        _f.write(_new_script.encode("utf-8"))
                    
                    print(f"updated to Version {_new_version}")
                    break
                elif _answer in ("D","d"):
                    for _line in difflib.unified_diff(_content.split("\n"),
                                _new_script.split("\n"),
                                fromfile=f"Version: {VERSION}",
                                fromfiledate=_current_last_modified.isoformat(),
                                tofile=f"Version: {_new_version}",
                                tofiledate=_github_last_modified.isoformat(),
                                n=0,
                                lineterm=""):
                        print(_line)
                else:
                    break
        else:
            ZFSCHECK_OBJ = zfscheck(**args.__dict__)
    except KeyboardInterrupt:
        print("")
        sys.exit(0)
    except Exception as e:
        print(str(e), file=sys.stderr)
        if args.debug:
            raise
        sys.exit(1)
