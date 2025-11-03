import ROOT

import xml.etree.ElementTree as ET

from collections import OrderedDict, defaultdict

xml_run_list="numu_22_23_1760364570.335776.xml"
out_dir="/eos/experiment/sndlhc/users/cvilela/numu_22_23_data_reprocess_legacyFilter"

xml_tree = ET.parse(xml_run_list)
xml_root = xml_tree.getroot()

stats = xml_root.find("meta").find("statistics")

stats_n_runs = int(stats.find("n_runs").text)
stats_n_events = int(stats.find("tot_n_events").text)

print("STATS: {} {}".format(stats_n_runs, stats_n_events))

run_data = []

all_runs = xml_root.find('runs').findall('run')

totals = defaultdict(int)

for run in all_runs:
    this_d = OrderedDict()
    
    this_d["run_number"] = int(run.find('run_number').text)
    this_d["run_n_events"] = int(run.find('n_events').text)
    
    totals["run_n_events"] += this_d["run_n_events"]

    this_d["n_stage1_start"] = -1
    this_d["n_stage1_end"] = -1
    this_d["n_stage1_tree"] = -1
    this_d["eff_stage1"] = -1
    this_d["n_stage2_start"] = -1
    this_d["n_stage2_end"] = -1
    this_d["n_stage2_tree"] = -1
    this_d["eff_stage2"] = -1

    try:
        f_stage1 = ROOT.TFile(out_dir+"/run_{0:06d}/snd_event_filter_stage1_{0:06d}.root".format(this_d["run_number"]))
        try:
            this_d["n_stage1_start"] = f_stage1["cutFlow"].GetBinContent(1)
            this_d["n_stage1_end"] = f_stage1["cutFlow"].GetBinContent(f_stage1["cutFlow"].GetNbinsX())
            this_d["eff_stage1"] = this_d["n_stage1_end"]/float(this_d["n_stage1_start"])

            this_d["n_stage1_tree"] = f_stage1["rawConv"].GetEntries()

            for field in ["n_stage1_start", "n_stage1_end", "n_stage1_tree"]:
                totals[field] += this_d[field]
                
        except KeyError:
            pass

        f_stage2 = ROOT.TFile(out_dir+"/run_{0:06d}/snd_event_filter_stage2_{0:06d}.root".format(this_d["run_number"]))
        try:
            this_d["n_stage2_start"] = f_stage2["cutFlow"].GetBinContent(1)
            this_d["n_stage2_end"] = f_stage2["cutFlow"].GetBinContent(f_stage2["cutFlow"].GetNbinsX())
            this_d["eff_stage2"] = this_d["n_stage2_end"]/float(this_d["n_stage2_start"])

            this_d["n_stage2_tree"] = f_stage2["rawConv"].GetEntries()

            for field in ["n_stage2_start", "n_stage2_end", "n_stage2_tree"]:
                totals[field] += this_d[field]
                
        except KeyError:
            pass
        
    except OSError:
        pass
    run_data.append(this_d)

from rich.console import Console
from rich.table import Table

headers = list(run_data[0].keys())
table = Table(title = "Process validation", header_style = "bold cyan")
for header in headers:
    table.add_column(header, justify="center", style="white")
for i, row in enumerate(run_data):
    colored_row = []
    for h in headers:
        this_color = "white"

        if row[h] == 0:
            this_color = "gray37"
        
        # Check Stage 1 starting point
        if h == "n_stage1_start":
            this_color = "green"
            if row[h] != row["run_n_events"]:
                this_color = "red"

        # Check Stage 1 tree
        if h == "n_stage1_tree":
            this_color = "green"
            if row[h] != row["n_stage1_end"]:
                this_color = "red"

        # Check Stage 2 starting point
        if h == "n_stage2_start":
            this_color = "green"
            if row[h] != row["n_stage1_end"]:
                this_color = "red"

        # Check Stage 2 tree
        if h == "n_stage2_tree":
            this_color = "green"
            if row[h] != row["n_stage2_end"]:
                this_color = "red"
        if row[h] == -1:
            this_color = "red"
        if h[:3] == "eff":
            colored_row.append(f"[{this_color}]{row[h]:.2e}[/{this_color}]")
        else:
            colored_row.append(f"[{this_color}]{int(row[h])}[/{this_color}]")

    table.add_row(*colored_row)

summary_row = ["", f'{int(totals["run_n_events"])}', f'{int(totals["n_stage1_start"])}', f'{int(totals["n_stage1_tree"])}', f'{int(totals["n_stage1_end"])}', "", f'{int(totals["n_stage2_start"])}', f'{int(totals["n_stage2_tree"])}', f'{int(totals["n_stage2_end"])}']
table.add_row(*summary_row, end_section=True)    

console = Console()
console.print(table)

exit()
