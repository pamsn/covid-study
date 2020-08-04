import csv
import warnings
from datetime import datetime
import os
import shutil
import subprocess
import threading
import sys
import time

import nltk
# nltk.download('stopwords')
# nltk.download('wordnet')
# print (sys.version)
import pandas as pd
warnings.simplefilter(action='ignore', category=FutureWarning)

start_date = '2019-01-01'
end_date = '2020-05-19'
dict_result = {}
project_gitcproc_list =[]

def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

def get_info(repositories,conf,projects_to_be_analyzed):

    for url in projects_to_be_analyzed:
        owner = url.split("/")[3]
        repo = url.split("/")[4].replace(".git", "")
        project_gitcproc_list.append(owner + "/" + repo)
    list_dicts=[]
    for repo in project_gitcproc_list:
        f=open(repositories,'w')
        f.write(repo+'\n')
        f.close()
        python2 = '/Users/belize/projetos_github/pandemia_project/env2/bin/python2.7 '
        gitcproc = '/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/logChunk/gitcproc.py'
        output= subprocess.check_output('python gitcproc.py' +" -wl -pl " + conf
                            ,cwd='/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/logChunk/', shell=True)
        # if os.path.exists('/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/downloaded/' + repo.replace("/","'__'")+ "/"):
        #     shutil.rmtree('/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/downloaded/' + repo.replace("/","'__'")+ "/")
        open(repositories, 'w').close()

        csv_file_gitcproc = "/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/Results/"+repo.replace("/","'__'")+"ChangeSummary.csv"
        print(csv_file_gitcproc)
        dataframe_project_commits = pd.read_csv(csv_file_gitcproc)

        print(dataframe_project_commits.size)
        dataframe_project_commits['date'] = dataframe_project_commits['commit_date'].str[1:11]
        dataframe_project_commits['date'] = pd.to_datetime(dataframe_project_commits['date'])
        mask = (dataframe_project_commits['date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & \
               (dataframe_project_commits['date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
        dataframe_project_commits = dataframe_project_commits.loc[mask]

        dataframe_commits_NO_bug = dataframe_project_commits[dataframe_project_commits['is_bug'] == "'False'"]
        # print(dataframe_commits_NO_bug.to_string())

        list_of_commits_NO_bugs = dataframe_commits_NO_bug['date'].tolist()
        # print(list_of_commits_NO_bugs)
        s = pd.to_datetime(pd.Series(list_of_commits_NO_bugs), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_NO_bugs = dict(zip(s.index.format(), s))
        result_dict_NO_bugs['url'] = 'https://github.com/' + repo+ '.git'

        # analyzed_projects = []
        # with open("/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/gitcproc_commit_NO_bug.csv") as csvfile:
        #     reader = csv.DictReader(csvfile, delimiter=",")
        #     for dct in map(dict, reader):
        #         analyzed_projects.append(dct)
        #
        # final = analyzed_projects + [result_dict_NO_bugs]

        with open("/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/gitcproc_commit_NO_bug.csv", 'a') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=['url', '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09',
                                                                  '2019-10', '2019-11', '2019-12', '2020-01', '2020-02', '2020-03', '2020-04', '2020-05'])
            dict_writer.writeheader()
            dict_writer.writerows([result_dict_NO_bugs])
        output_file.close()
        final = []

        dataframe_commits_bug = dataframe_project_commits[dataframe_project_commits['is_bug'] == "'True'"]
        # print(dataframe_commits_bug.to_string())

        list_of_commits_bugs = dataframe_commits_bug['date'].tolist()
        s = pd.to_datetime(pd.Series(list_of_commits_bugs), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_bugs = dict(zip(s.index.format(), s))
        result_dict_bugs['url'] = 'https://github.com/' + repo+ '.git'

        # analyzed_projects = []
        # with open("/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/gitcproc_commit_bug.csv") as csvfile:
        #     reader = csv.DictReader(csvfile, delimiter=",")
        #     for dct in map(dict, reader):
        #         analyzed_projects.append(dct)
        #
        # final = analyzed_projects + [result_dict_bugs]

        with open("/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/gitcproc_commit_bug.csv", 'a') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=['url', '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09',
                                                                  '2019-10', '2019-11', '2019-12', '2020-01', '2020-02', '2020-03', '2020-04', '2020-05'])
            dict_writer.writeheader()
            dict_writer.writerows([result_dict_bugs])
        output_file.close()

        # analyzed_projects = []
        # with open("/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/gitcproc_commit_bug.csv") as csvfile:
        #     reader = csv.DictReader(csvfile, delimiter=",")
        #     for dct in map(dict, reader):
        #         analyzed_projects.append(dct)
        #
        # result = list(set(projects_to_be_analyzed) - set([d['url'] for d in analyzed_projects]))
        # print("/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/Results/"+repo.replace("/","'__'")+"ChangeSummary.csv")
        # print("/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/Results/" + repo.replace("/", "'__'") + "PatchSummary.csv")
        os.remove("/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/Results/"+repo.replace("/","'__'")+"ChangeSummary.csv")
        os.remove("/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/Results/" + repo.replace("/", "'__'") + "PatchSummary.csv")


        # for project in result:
        #     owner = project.split("/")[3]
        #     repo = project.split("/")[4].replace(".git", "")
        #     project_gitcproc_list.append(owner + "/" + repo)



analyzed_projects1 = []

projects_to_be_analyzed = [line.rstrip('\n') for line in open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/projects_to_analyze_clone.txt')]
final = []
# with open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/gitcproc_commit_bug.csv') as csvfile:
#     reader = csv.DictReader(csvfile, delimiter=",")
#     for dct in map(dict, reader):
#         analyzed_projects1.append(dct)
#
# result = list(set(projects_to_be_analyzed) - set([d['url'] for d in analyzed_projects1]))

final = chunkIt(projects_to_be_analyzed,1)

t1 = threading.Thread(name='t1', target=get_info, args=('/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/logChunk/repositories1.txt',
                                                            "/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/demo_conf1.ini",
                                                            final[0]))
# t2 = threading.Thread(name='t2', target=get_info, args=('/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/logChunk/repositories2.txt',
#                                                              "/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/demo_conf2.ini",
#                                                              final[1]))
# t3 = threading.Thread(name='t3', target=get_info, args=('/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/gitcproc-master/src/logChunk/repositories3.txt',
#                                                             "/Users/belize/projetos_github/pandemia_project/gitcproc_analysis/demo_conf3.ini",
#                                                              final[2]))

t1.start()
# t2.start()
# t3.start()

# t1.join()
# t2.join()
# t3.join()

