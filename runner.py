#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileCopyrightText: 2026 GOTUNIX Networks <code@gotunix.net>
# SPDX-FileCopyrightText: 2026 Justin Ovens <code@gotunix.net>
# ----------------------------------------------------------------------------------------------- #
#                 $$$$$$\   $$$$$$\ $$$$$$$$\ $$\   $$\ $$\   $$\ $$$$$$\ $$\   $$\               #
#                $$  __$$\ $$  __$$\\__$$  __|$$ |  $$ |$$$\  $$ |\_$$  _|$$ |  $$ |              #
#                $$ /  \__|$$ /  $$ |  $$ |   $$ |  $$ |$$$$\ $$ |  $$ |  \$$\ $$  |              #
#                $$ |$$$$\ $$ |  $$ |  $$ |   $$ |  $$ |$$ $$\$$ |  $$ |   \$$$$  /               #
#                $$ |\_$$ |$$ |  $$ |  $$ |   $$ |  $$ |$$ \$$$$ |  $$ |   $$  $$<                #
#                $$ |  $$ |$$ |  $$ |  $$ |   $$ |  $$ |$$ |\$$$ |  $$ |  $$  /\$$\               #
#                \$$$$$$  | $$$$$$  |  $$ |   \$$$$$$  |$$ | \$$ |$$$$$$\ $$ /  $$ |              #
#                 \______/  \______/   \__|    \______/ \__|  \__|\______|\__|  \__|              #
# ----------------------------------------------------------------------------------------------- #
# Copyright (C) GOTUNIX Networks                                                                  #
# Copyright (C) Justin Ovens                                                                      #
# ----------------------------------------------------------------------------------------------- #
# This program is free software: you can redistribute it and/or modify                            #
# it under the terms of the GNU Affero General Public License as                                  #
# published by the Free Software Foundation, either version 3 of the                              #
# License, or (at your option) any later version.                                                 #
#                                                                                                 #
# This program is distributed in the hope that it will be useful,                                 #
# but WITHOUT ANY WARRANTY; without even the implied warranty of                                  #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                                   #
# GNU Affero General Public License for more details.                                             #
#                                                                                                 #
# You should have received a copy of the GNU Affero General Public License                        #
# along with this program.  If not, see <https://www.gnu.org/licenses/>.                          #
# ----------------------------------------------------------------------------------------------- #
import argparse
import asyncio
import sys
import time
import getpass
import os
import yaml
from collections import defaultdict
from rich.live import Live
from rich.table import Table
from rich.console import Console

console = Console()

class JobState:
    WAITING = "[gray]Waiting[/gray]"
    RUNNING = "[blue]Running...[/blue]"
    SUCCESS = "[green]Success[/green]"
    FAILED = "[red]Failed[/red]"
    SKIPPED = "[yellow]Skipped[/yellow]"

class Job:
    def __init__(self, group_name, name, command, env=None):
        self.group_name = group_name
        self.name = name
        self.command = command
        self.env = env or {}
        self.state = JobState.WAITING
        self.duration = 0.0
        self.stdout = ""
        self.stderr = ""

class JobGroup:
    def __init__(self, name, priority, wait_for_completion, run_on="success"):
        self.name = name
        self.priority = priority
        self.wait_for_completion = wait_for_completion
        self.run_on = run_on
        self.jobs = []

    async def run(self):
        for job in self.jobs:
            success = await self._run_command(job)
            if not success:
                found = False
                for j in self.jobs:
                    if j == job:
                        found = True
                    elif found and j.state == JobState.WAITING:
                        j.state = JobState.SKIPPED
                break

    async def _run_command(self, job):
        job.state = JobState.RUNNING
        loop = asyncio.get_event_loop()
        start_time = loop.time()
        
        run_env = os.environ.copy()
        
        # Build dynamic vars specifically for execution phase safely
        dynamic_vars = build_dynamic_vars(job.env)
        dynamic_vars["${JOB_NAME}"] = job.name
        dynamic_vars["${JOB_GROUP}"] = job.group_name
        dynamic_vars["${LOGFILE}"] = f"{job.group_name}_{job.name}_{int(time.time())}.log"
        
        # Extra evaluation pass in case LOGFILE relies on something deeply chained
        for _ in range(2):
            for key in dynamic_vars.keys():
                for dyn_k, dyn_v in dynamic_vars.items():
                    if key != dyn_k and dyn_k in dynamic_vars[key]:
                        dynamic_vars[key] = dynamic_vars[key].replace(dyn_k, dynamic_vars[dyn_k])
        
        if job.env:
            for k in job.env.keys():
                # Extract fully resolved local custom variables
                run_env[k] = dynamic_vars.get(f"${{{k}}}", job.env[k])
        
        cmd_to_run = job.command
        for dyn_k, dyn_v in dynamic_vars.items():
            cmd_to_run = cmd_to_run.replace(dyn_k, dyn_v)
        
        process = await asyncio.create_subprocess_shell(
            cmd_to_run,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=run_env
        )
        stdout, stderr = await process.communicate()
        
        job.duration = loop.time() - start_time
        job.stdout = stdout.decode().strip()
        job.stderr = stderr.decode().strip()
        
        if process.returncode == 0:
            job.state = JobState.SUCCESS
            return True
        else:
            job.state = JobState.FAILED
            return False

def build_dynamic_vars(job_env_dict):
    """Builds a fully resolved dictionary of variables from OS + defaults + job env"""
    dynamic_vars = {}
    
    # OS Environment fallback
    for k, v in os.environ.items():
        dynamic_vars[f"${{{k}}}"] = str(v)
        
    dynamic_vars["${TIMESTAMP}"] = str(int(time.time()))
    dynamic_vars["${USER}"] = getpass.getuser()
    dynamic_vars["${PWD}"] = os.getcwd()
    
    if job_env_dict:
        for k, v in job_env_dict.items():
            dynamic_vars[f"${{{k}}}"] = str(v)
            
    for _ in range(3):
        for key in dynamic_vars.keys():
            for dyn_k, dyn_v in dynamic_vars.items():
                if key != dyn_k and dyn_k in dynamic_vars[key]:
                    dynamic_vars[key] = dynamic_vars[key].replace(dyn_k, dynamic_vars[dyn_k])
                    
    return dynamic_vars

def resolve_string(target_str, dynamic_vars):
    if not isinstance(target_str, str):
        return target_str
    res = target_str
    for dyn_k, dyn_v in dynamic_vars.items():
        if dyn_k in res:
            res = res.replace(dyn_k, dyn_v)
    return res

def generate_table(all_jobs, run_name=None):
    title = "[white on default]Background Job Runner Status[/white on default]"
    if run_name:
        title += f"\n[white on default]{run_name}[/white on default]"
        
    table = Table(title=title, show_header=True, header_style="bold white on default")
    table.add_column("Priority", style="green", width=10, justify="center")
    table.add_column("Group", style="cyan", width=30)
    table.add_column("Job Name", style="magenta", width=40)
    table.add_column("Status", justify="center", width=15)
    table.add_column("Duration", justify="right", width=10)
    
    for job, priority in all_jobs:
        duration_str = f"{job.duration:.2f}s" if job.duration > 0 else "-"
        table.add_row(str(priority), job.group_name, job.name, job.state, duration_str)
        
    return table

async def main():
    parser = argparse.ArgumentParser(description="Run background jobs concurrently with priority tiers.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--verbose", action="store_true", help="Print detailed stdout/stderr output on failure")
    parser.add_argument("--verbose-all", action="store_true", help="Print detailed stdout/stderr output for ALL jobs")
    parser.add_argument("--cron", action="store_true", help="Run without interactive UI")
    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        console.print(f"[red]Error reading config file: {e}[/red]")
        sys.exit(1)

    raw_run_name = config.get("name")
    if raw_run_name:
        top_level_vars = build_dynamic_vars(config.get('env', {}))
        run_name_title = resolve_string(str(raw_run_name), top_level_vars)
    else:
        run_name_title = None

    jobs_config = config.get("jobs", [])
    if not jobs_config:
        console.print("[yellow]Warning: No jobs found in 'jobs' array.[/yellow]")
        sys.exit(0)
    
    tiers = defaultdict(list)
    all_jobs_for_ui = []

    for j_dict in jobs_config:
        priority_env = j_dict.get('env', {})
        cluster_vars = build_dynamic_vars(priority_env)
        
        raw_priority = resolve_string(str(j_dict.get('priority', '99')), cluster_vars)
        try: 
            priority = int(raw_priority)
        except ValueError: 
            priority = 99
            
        wait_flag = str(j_dict.get('wait_for_completion', 'False')).lower() == 'true'
        run_on = str(j_dict.get('run_on', 'success')).lower()
        
        if 'cluster' in j_dict:
            cluster_name = resolve_string(str(j_dict['cluster']), cluster_vars)
            group = JobGroup(cluster_name, priority, wait_flag, run_on)
            for t in j_dict.get('tasks', []):
                t_env = {**priority_env, **t.get('env', {})}
                task_vars = build_dynamic_vars(t_env)
                
                t_name = resolve_string(str(t['name']), task_vars)
                job = Job(group.name, t_name, t['command'], t_env)
                group.jobs.append(job)
                all_jobs_for_ui.append((job, priority))
            tiers[priority].append(group)
        else:
            t_env = j_dict.get('env', {})
            task_vars = build_dynamic_vars(t_env)
            t_name = resolve_string(str(j_dict.get('name', 'unnamed')), task_vars)
            
            group = JobGroup("standalone", priority, wait_flag, run_on)
            job = Job("standalone", t_name, j_dict['command'], t_env)
            group.jobs.append(job)
            all_jobs_for_ui.append((job, priority))
            tiers[priority].append(group)

    sorted_priorities = sorted(tiers.keys())
    all_jobs_for_ui.sort(key=lambda x: x[1])

    async def engine():
        pipeline_failed = False
        
        for p in sorted_priorities:
            groups_in_tier = tiers[p]
            running_tasks = []
            
            for group in groups_in_tier:
                if group.wait_for_completion and running_tasks:
                    await asyncio.gather(*running_tasks)
                    running_tasks = []
                    # Dynamically check for fail-state triggered before barrier
                    for g in groups_in_tier:
                        for j in g.jobs:
                            if j.state == JobState.FAILED:
                                pipeline_failed = True
                                
                should_run = False
                if group.run_on == 'always':
                    should_run = True
                elif group.run_on == 'success' and not pipeline_failed:
                    should_run = True
                elif group.run_on == 'failure' and pipeline_failed:
                    should_run = True
                    
                if not should_run:
                    for j in group.jobs:
                        j.state = JobState.SKIPPED
                    continue

                if group.wait_for_completion:
                    await group.run()
                    # Check post-barrier success
                    for j in group.jobs:
                        if j.state == JobState.FAILED:
                            pipeline_failed = True
                else:
                    running_tasks.append(asyncio.create_task(group.run()))
                    
            if running_tasks:
                await asyncio.gather(*running_tasks)
                
            # Scan full tier status to update health before proceeding globally
            for group in groups_in_tier:
                for j in group.jobs:
                    if j.state == JobState.FAILED:
                        pipeline_failed = True

    if args.cron:
        console.print("[cyan]Running in CRON mode...[/cyan]")
        await engine()
        console.print("\n[green]Execution Completed. Final Status:[/green]")
        console.print(generate_table(all_jobs_for_ui, run_name=run_name_title))
    else:
        with Live(generate_table(all_jobs_for_ui, run_name=run_name_title), refresh_per_second=4, console=console) as live:
            async def ui_updater(engine_task):
                while not engine_task.done():
                    live.update(generate_table(all_jobs_for_ui, run_name=run_name_title))
                    await asyncio.sleep(0.2)
                live.update(generate_table(all_jobs_for_ui, run_name=run_name_title))
                
            engine_t = asyncio.create_task(engine())
            updater_t = asyncio.create_task(ui_updater(engine_t))
            
            await engine_t
            await updater_t

    if args.verbose_all or args.verbose:
        target_jobs = [j for j, _ in all_jobs_for_ui if j.state not in (JobState.WAITING, JobState.SKIPPED)]
        if not args.verbose_all:
            target_jobs = [j for j in target_jobs if j.state == JobState.FAILED]
            
        if target_jobs:
            title = "All Executed Job Details" if args.verbose_all else "Failed Job Details"
            color = "cyan bold" if args.verbose_all else "red bold"
            console.print(f"\n[{color}]{title} (Verbose)[/{color}]")
            
            for fj in target_jobs:
                console.print(f"\n[magenta]--- {fj.name} ({fj.state.replace('[','').replace(']','').split('}')[-1] if '}' in fj.state else fj.state.split(']')[1].split('[')[0]}) ---[/magenta]")
                if fj.stdout: console.print(f"[dim]STDOUT:[/dim]\n{fj.stdout}")
                if fj.stderr: console.print(f"[red]STDERR:[/red]\n{fj.stderr}")
                
        if any(j.state == JobState.FAILED for j, _ in all_jobs_for_ui):
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
