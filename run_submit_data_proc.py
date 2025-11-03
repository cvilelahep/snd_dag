#!/usr/bin/env python3
from pathlib import Path
import htcondor
import os
import shutil

import xml.etree.ElementTree as ET

import numpy as np

# API name changed in newer releases (htcondor2). Try the current one first.
try:
    from htcondor import dags
except Exception:
    from htcondor2 import dags  # fallback for newer installs

dry = True
    
xml_run_list="numu_22_23_1760364570.335776.xml"
#xml_run_list="/afs/cern.ch/work/c/cvilela/private/snd_dag/numu_SHORT.xml"

VARS = {
    "TAG": "numu_22_23_data_reprocess_legacyFilter_stage1_stage2",
    "CONDOR_FOLDER": "/afs/cern.ch/work/c/cvilela/private/snd_dag",
    "OUTPUTDIR": "/eos/experiment/sndlhc/users/cvilela/numu_22_23_data_reprocess_legacyFilter_stage1_stage2"
}

shell_script = VARS["CONDOR_FOLDER"]+"/run_neutrino_list.sh"

outdir = Path(VARS["OUTPUTDIR"]).resolve()
outdir.mkdir(parents=True, exist_ok=True)

base = Path(VARS["CONDOR_FOLDER"]).resolve()
tag_suffix = VARS["TAG"].split("/")[-1]
dag_dir = base / f"dag_{tag_suffix}"

log_dir = dag_dir

xml_tree = ET.parse(xml_run_list)
xml_root = xml_tree.getroot()

all_runs = xml_root.find('runs').findall('run')

d = []
max_size = 0
tot_size = 0

for run in all_runs:
    run_number = int(run.find('run_number').text)
    n_files = int(run.find('n_files').text)
    if n_files > max_size:
        max_size = n_files
    tot_size += n_files
    path = run.find('path').text.replace("raw_data", "convertedData")
    year = int(run.find('start').text[:4])
    d.append([path, n_files, run_number, year])

max_N = 50
print(max_size, tot_size)

# Group runs in sequences not exceeding the largest run size
grouped_runs = [[]]
grouped_runs_size = [0]
last_year = -1

for p, s, directory, year in d:
    if (last_year != -1) and (last_year != year):
        new_year = True
    else:
        new_year = False
    last_year = year
    if grouped_runs_size[-1] + s > max_size or len(grouped_runs[-1]) + 1 > max_N or new_year:
        grouped_runs.append([])
        grouped_runs_size.append(0)
    grouped_runs[-1].append(p)
    grouped_runs_size[-1] += s
    
# Cross check:
grouped_total_runs = 0
grouped_total_size = 0
for g in grouped_runs:
    grouped_total_runs += len(g)
for g in grouped_runs_size:
    grouped_total_size += g

print("N JOBS {}".format(len(grouped_runs)))
print("{} - {} = {}".format(grouped_total_runs, len(d), grouped_total_runs - len(d)))
print("{} - {} = {}".format(grouped_total_size, tot_size, grouped_total_size - tot_size))

itemdata = {}
for i_job, (job_name, this_mode, this_cutset) in enumerate((("stage1", "FILTER", "/analysis/analyses/snd_analysis_2025_numu_22_23/pipelines/allowWalls2and5_legacyScifiFilter.h"),
                                                          ("ds_track_reco", "RECO", "NONE"),
                                                          ("stage2", "FILTER", "/analysis/analyses/snd_analysis_2025_numu_22_23/pipelines/stage2cuts_legacyScifiFilter.h"))):
    
    itemdata[job_name] = []
    
    for i, g in enumerate(grouped_runs):
        run_string = ""
        job_complete = True
        for p in g:
            if i_job == 0:
                run_string += p
            else:
                path = Path(p)
                dirname = path.parent.name if not path.is_dir() else path.name
                run_string += VARS["OUTPUTDIR"]+"/"+dirname
            run_string += ","
        run_string = run_string[:-1]

        if "2023" in p:
            this_geofile = "/eos/experiment/sndlhc/convertedData/physics/2023/geofile_sndlhc_TI18_V3_2023.root"
        elif "2022" in p:
            this_geofile = "/eos/experiment/sndlhc/convertedData/physics/2022/geofile_sndlhc_TI18_V4_2022.root"
        else:
            print(run_string)
            raise RuntimeError("Couldn't determine if runs are 2022 or 2023 to get correct geo file")
        
        if job_name in ["ds_track_reco", "stage2"]:
            this_input_base = "snd_event_filter_stage1"
            this_reco_base = "snd_event_filter_stage1"
        else:
            this_input_base = "NONE"
            this_reco_base = "NONE"
        
            
        itemdata[job_name].append({"out_dir" : VARS["OUTPUTDIR"],
                                   "run_string" : run_string,
                                   "mode": this_mode,
                                   "cutset": this_cutset,
                                   "procname": job_name,
                                   "geofile": this_geofile,
                                   "input_base" : this_input_base,
                                   "reco_base": this_reco_base,
                                   "item_index": i})
condor_jobs = {}
for k, i in itemdata.items():
    condor_jobs[k] = htcondor.Submit({
        'executable' : '/bin/bash',
        'arguments': shell_script+' $(out_dir) $(run_string) $(mode) $(cutset) $(procname) $(geofile) $(input_base) $(reco_base)',
        'output': (log_dir / 'sndjob_{}_$(item_index).out'.format(k)).as_posix(),
        'error': (log_dir / 'sndjob_{}_$(item_index).err'.format(k)).as_posix(),
        'log': (log_dir / 'sndjob_{}_$(item_index).log'.format(k)).as_posix(),
        'request_CPUs': '1',
        'should_transfer_files': 'NO',
        '+JobFlavour': '"tomorrow"',
#        '+JobFlavour': '"microcentury"',
        'MY.SendCredential': True,
        'requirements': '(OpSysAndVer =?= \"AlmaLinux9\")',
        '+AccountingGroup': 'group_u_SNDLHC.users'})

DAG_NAME = VARS["TAG"]+".dag"
DOT_PATH = VARS["TAG"]+".dot"


# CLEAN first, then recreate dag_dir
shutil.rmtree(dag_dir, ignore_errors=True)
dag_dir.mkdir(parents=True, exist_ok=True)

# One logical node per layer (vars is a list; one dict == one underlying node)
node_vars = [VARS]

# DOT config like:  DOT dag.dot UPDATE
dot_cfg = dags.DotConfig(path=DOT_PATH, update=True)

dag = dags.DAG(dot_config=dot_cfg)

dag_layer_stage1 = dag.layer(
    name="stage1",
    submit_description=condor_jobs["stage1"],
    vars=itemdata["stage1"],
    dir=dag_dir,                       # submit from your workflow folder
    retry_unless_exit=0,
    retries=5,
)
dag_layer_ds_track_reco = dag_layer_stage1.child_layer(
    name="ds_track_reco",
    submit_description=condor_jobs["ds_track_reco"],
    vars=itemdata["ds_track_reco"],
    dir=dag_dir,
    edge=dags.OneToOne(),
    retry_unless_exit=0,
    retries=5,
)

dag_layer_stage2= dag_layer_ds_track_reco.child_layer(
    name="stage2",
    submit_description=condor_jobs["stage2"],
    vars=itemdata["stage2"],
    dir=dag_dir,
    edge=dags.OneToOne(),
    retry_unless_exit=0,
    retries=5,
)

print(dag.describe())

dag_file = dags.write_dag(dag, dag_dir=dag_dir, dag_file_name=DAG_NAME)

# Change cwd BEFORE building the Submit from the DAG (mimics condor_submit_dag)
os.chdir(dag_dir)

dag_submit = htcondor.Submit.from_dag(str(dag_file))

# Problems submitting directly from python.
print("DAG FILES READY")
print("RUN: cd "+dag_dir.as_posix()+"; condor_submit_dag -f "+DAG_NAME+";")

exit() 
# Optional: if you use Kerberos creds
# Push your Kerberos ticket to the credd (uses your current kinit cache)
col = htcondor.Collector()
#schedd_ad = col.locate(htcondor.DaemonTypes.Schedd, "bigbird25.cern.ch")
credd = htcondor.Credd()
credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
dag_submit["MY.SendCredential"] = "True"
dag_submit["getenv"] = "True"
if not dry:
    schedd = htcondor.Schedd()
    res = schedd.submit(dag_submit)
    cluster_id = getattr(res, "cluster", lambda: int(res))()
    print(f"DAGMan job cluster is {cluster_id}")
else:
    print(dag_submit)
