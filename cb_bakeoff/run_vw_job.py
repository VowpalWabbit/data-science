#! /usr/bin/env python3

import argparse
import os
import re
import shutil
import subprocess
import sys
import time
import csv
import azure.storage.blob as azureblob
#import pandas as pd
from subprocess import call
#from config import BAKEOFF_BASE_DIR

USE_ADF = True
USE_CS = False
'''
if shutil.which("vw") is None:
    print("FATAL: vw not found in path", file=sys.stderr)
    sys.exit(1)
'''

BAKEOFF_BASE_DIR = os.getcwd()

if USE_CS:
    VW_DS_DIR = os.path.join(BAKEOFF_BASE_DIR,
                             "cb_eval",
                             "vwshuffled_cs")
    DIR_PATTERN = os.path.join(BAKEOFF_BASE_DIR,
                               "cb_eval",
                               "res_cs",
                               "cbresults_{}")
else:
    VW_DS_DIR = os.path.join(BAKEOFF_BASE_DIR,
                             "cb_eval",
                             "vwshuffled")
    DIR_PATTERN = os.path.join(BAKEOFF_BASE_DIR,
                               "cb_eval",
                               "res",
                               "cbresults_{}")

rgx = re.compile('^average loss = (.*)$', flags=re.M)

def expand_cover(policies):
    algs = []
    for psi in [0, 0.01, 0.1, 1.0]:
        algs.append(('cover', policies, 'psi', psi))
        algs.append(('cover', policies, 'psi', psi, 'nounif', None))
        # algs.append(('cover', policies, 'psi', psi, 'nounif', None, 'mellowness', 0.1))
        # algs.append(('cover', policies, 'psi', psi, 'nounif', None, 'mellowness', 0.01))
    return algs

params = {
    'alg': [
        #('epsilon', 0),
        #('epsilon', 0.02),
        #('epsilon', 0.05),
        #('epsilon', 0.1)
        ('epsilon', 0.2)
        ],
    'coin' : ['--coin', ''],
    #'learning_rate': [1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 0.001, 0.003, 0.01, 0.03, 0.1, 0.5, 1.0, 10.0],
    'learning_rate': [0.001],
    #'power_t': [0, 0.5],
    'power_t': [0],
    #'cb_type': ['dr', 'ips', 'mtr'],
    'cb_type': ['mtr'],
    'clip_p': [0.0, 0.1, 0.5]
    }

extra_flags = None
#extra_flags = ['--loss0', '9', '--loss1', '10', '--baseline']
#extra_flags = ['--coin']

def param_grid():
    grid = [{}]
    for k in params:
        new_grid = []
        #print("grid: ", grid)
        for g in grid:
            for param in params[k]:
                #print("k, param: ", k, param)
                gg = g.copy()
                if k not in ['learning_rate','power_t']:
                    gg[k] = param
                else:
                    if 'coin' in gg and gg['coin'] == '':
                        gg[k] = param
                new_grid.append(gg)
        #print("new grid: ", new_grid)
        grid = new_grid

    return list(sorted(grid, key = lambda x: list(sorted(x.items()))))


def ds_files():
    import glob
    return list(sorted(glob.glob(os.path.join(BAKEOFF_BASE_DIR,'*.vw.gz'))))
    #return list(sorted(glob.glob(os.path.join(BAKEOFF_BASE_DIR, '*.vw.gz'))))


def get_task_name(ds, params):
    did, n_actions = os.path.basename(ds).split('.')[0].split('_')[1:]
    did, n_actions = int(did), int(n_actions)

    task_name = 'ds:{}|na:{}'.format(did, n_actions)
    if len(params) > 1:
        for k, v in sorted(params.items()):
            if k!= 'alg':
                if k!= 'coin':
                    task_name += '|'+'{}:{}'.format(k, str(v))
    #task_name += '|' + ':'.join([str(p) for p in params['alg'] if p is not None])
    p1 = params['alg'][::2]
    p2 = params['alg'][1::2]
    
    #print("p1, p2: ",p1,p2)
    for p in range(len(p1)):
        task_name += '|' + str(p1[p]) + ':' + str(p2[p])
    if 'coin' in params:
        task_name += '|' + "coin:" + '"'+str(params['coin'])+'"'
        
    return task_name


def process(ds, params, results_dir):
    print('processing', ds, params)
    did, n_actions = os.path.basename(ds).split('.')[0].split('_')[1:]
    did, n_actions = int(did), int(n_actions)

    #cmd = [shutil.which("vw"), ds, '-b', '24']
    cmd = ["./vw", ds]
    for k, v in params.items():
        if k == 'alg':
            if v[0] == 'supervised':
                cmd += ['--csoaa' if USE_CS else '--oaa', str(n_actions)]
            else:
                cmd += ['--cbify', str(n_actions)]
                if USE_CS:
                    cmd += ['--cbify_cs']
                if extra_flags:
                    cmd += extra_flags
                if USE_ADF:
                    cmd += ['--cb_explore_adf']
                assert len(v) % 2 == 0, 'params should be in pairs of (option, value)'
                for i in range(len(v) // 2):
                    cmd += ['--{}'.format(v[2 * i])]
                    if v[2 * i + 1] is not None:
                        cmd += [str(v[2 * i + 1])]
        elif k =='coin':
            cmd += [v]
        else:
            if params['alg'][0] == 'supervised' and k == 'cb_type':
                pass
            else:
                cmd += ['--{}'.format(k), str(v)]

    print('running', cmd)
    t = time.time()
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('ascii')
    sys.stderr.write('\n\n{}, {}, time: {}, output:\n'.format(ds, params, time.time() - t))
    sys.stderr.write(output)
    pv_loss = float(rgx.findall(output)[0])
    print('elapsed time:', time.time() - t, 'pv loss:', pv_loss)

    return pv_loss

def export_dict_list_to_csv(data, filename):
    with open(filename, 'w') as f:
        # Assuming that all dictionaries in the list have the same keys.
        headers = sorted([k for k, v in data[0].items()])
        csv_data = [headers]

        for d in data:
            temp_data = []
            for h in headers:
                if h not in d and h in ['learning_rate','power_t']:
                    temp_data.append('None')
                else:
                    temp_data.append(d[h])
            csv_data.append(temp_data)

        writer = csv.writer(f)
        writer.writerows(csv_data)
        
def upload_to_blob(blob_client, container_name, file, ds, experiment_name):
    blob_name = str(experiment_name)+'/'+str(ds)+str(file)
    #print("blob name: ", blob_name)
    print('Uploading file {} to container [{}]...'.format(file,container_name))
    
    blob_client.create_blob_from_path(container_name,blob_name, os.path.basename(file))
    

if __name__ == '__main__':

    print("enter main....")
    parser = argparse.ArgumentParser(description='vw job')
    parser.add_argument('data_url',type=str)
    parser.add_argument('vw_path',type=str)
    parser.add_argument('task_id', type=int, help='task ID, between 0 and num_tasks - 1')
    parser.add_argument('num_tasks', type=int)
    parser.add_argument('storage_account_name',type=str)
    parser.add_argument('storage_account_key',type=str)
    parser.add_argument('container_name',type=str)
    parser.add_argument('experiment_name',type=str)
    parser.add_argument('--task_offset', type=int, default=0,
                        help='offset for task_id in output filenames')
    parser.add_argument('--results_dir', default=DIR_PATTERN.format('agree01'))
    parser.add_argument('--name', default=None)
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--flags', default=None, help='extra flags for cb algorithms')
    args = parser.parse_args()

    #print("Data URL: ", args.data_url)
    #print("task id: ", args.task_id)
    #print("num tasks: ", args.num_tasks)
    filename = args.data_url.split('?')[0].split('/')[-1]
    call('wget \"'+args.data_url+"\" -O "+filename, shell=True)
    call('wget \"'+args.vw_path+"\" -O vw", shell=True)
    call('chmod a+rx vw',shell=True)

    call('ls',shell=True)
    if args.name is not None:
        args.results_dir = DIR_PATTERN.format(args.name)

    if args.flags is not None:
        extra_flags = args.flags.split()
    grid = param_grid()
    #print("grid len: ", len(grid))
    #print(grid)
    dss = ds_files()
    #print("dss: ", dss)
    tot_jobs = len(grid) * len(dss)
    #print("tots_jobs: ", tot_jobs)

    if args.task_id == 0:
        if not os.path.exists(args.results_dir):
            os.makedirs(args.results_dir)
            import stat
            try:
                os.chmod(args.results_dir, os.stat(args.results_dir).st_mode | stat.S_IWOTH)
            except:
                pass
    else:
        while not os.path.exists(args.results_dir):
            time.sleep(1)
    if not args.test:
        fname = os.path.join(args.results_dir, 'loss{}.txt'.format(args.task_offset + args.task_id))
        done_tasks = set()
        if os.path.exists(fname):
            done_tasks = set([line.split()[0] for line in open(fname).readlines()])
        loss_file = open(fname, 'a')
    idx = args.task_id
    while idx < tot_jobs:
        print("idx, len(grid), idx//len(grid), idx%len(grid): ", idx, len(grid),idx//len(grid),idx % len(grid))
        ds = dss[idx // len(grid)]
        #print("ds: ", ds)
        params = grid[idx % len(grid)]
        if args.test:
            print(ds, params)
        else:
            #print("params: ", params)
            task_name = get_task_name(ds, params)
            print("printing task names.....")
            print(task_name)
            if task_name not in done_tasks:
                try:
                    pv_loss = process(ds, params, args.results_dir)
                    loss_file.write('{} {}\n'.format(task_name, pv_loss))
                    loss_file.flush()
                    os.fsync(loss_file.fileno())
                except subprocess.CalledProcessError:
                    sys.stderr.write('\nERROR: TASK FAILED {} {}\n\n'.format(ds, params))
                    print('ERROR: TASK FAILED', ds, params)
        idx += args.num_tasks
    
    if not args.test:
        loss_file.close()
        
    # format loss file to a csv
    loss_file = open(fname, 'r')
    loss_data = loss_file.readlines()
    ld1 = [x.strip("\n").split(" ")[0].split("|") for x in loss_data]
    ld2 = [{k:v for k,v in (y.split(':') for y in x)} for x in ld1]
    loss_list = [x.strip("\n").split(" ")[1] for x in loss_data]
    for i,dic in enumerate(ld2): dic.update({"loss": loss_list[i]})
    
    #df = pd.DataFrame(ld2)
    #df.to_csv('loss_formatted.csv',index=False)
    export_dict_list_to_csv(ld2, 'loss_formatted.csv')
    
    blob_client = azureblob.BlockBlobService(account_name=args.storage_account_name,account_key=args.storage_account_key)
    
    upload_to_blob(blob_client, args.container_name, 'loss_formatted.csv', os.path.basename(ds).split('.')[0].split('_')[1], args.experiment_name)
